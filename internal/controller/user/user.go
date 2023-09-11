/*
Copyright 2020 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package user

import (
	"context"
	"strings"

	v1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/event"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/meta"
	"github.com/crossplane/crossplane-runtime/pkg/password"
	"github.com/crossplane/crossplane-runtime/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	"github.com/crossplane-contrib/provider-kafka/apis/user/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-kafka/apis/v1alpha1"
	"github.com/crossplane-contrib/provider-kafka/internal/clients/kafka"
	"github.com/crossplane-contrib/provider-kafka/internal/clients/kafka/user"
)

const (
	errNotUser            = "managed resource is not a User custom resource"
	errTrackPCUsage       = "cannot track ProviderConfig usage"
	errGetPC              = "cannot get ProviderConfig"
	errGetCreds           = "cannot get credentials"
	errGetUser            = "cannot get user spec from user client"
	errGeneratePassword   = "cannot generate password"
	errNewClient          = "cannot create new Kafka client"
	errUpdateNotSupported = "updates are not supported"
)

// Setup adds a controller that reconciles User managed resources.
func Setup(mgr ctrl.Manager, l logging.Logger) error {
	name := managed.ControllerName(v1alpha1.UserGroupKind)

	o := controller.Options{
		RateLimiter: ratelimiter.NewController(),
	}

	r := managed.NewReconciler(mgr,
		resource.ManagedKind(v1alpha1.UserGroupVersionKind),
		managed.WithExternalConnectDisconnecter(&connectDisconnector{
			kube:         mgr.GetClient(),
			usage:        resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
			log:          l,
			newServiceFn: kafka.NewAdminClient}),
		managed.WithLogger(l.WithValues("controller", name)),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))))

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o).
		For(&v1alpha1.User{}).
		Complete(r)
}

// A connectDisconnector is expected to produce an ExternalClient when its Connect method
// is called and close it when its Disconnect method is called.
type connectDisconnector struct {
	kube         client.Client
	usage        resource.Tracker
	log          logging.Logger
	newServiceFn func(ctx context.Context, creds []byte, kube client.Client) (*kadm.Client, error)
	cachedClient *kadm.Client
}

// Connect typically produces an ExternalClient by:
// 1. Tracking that the managed resource is using a ProviderConfig.
// 2. Getting the managed resource's ProviderConfig.
// 3. Getting the credentials specified by the ProviderConfig.
// 4. Using the credentials to form a client.
func (c *connectDisconnector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	cr, ok := mg.(*v1alpha1.User)
	if !ok {
		return nil, errors.New(errNotUser)
	}

	if err := c.usage.Track(ctx, mg); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	pc := &apisv1alpha1.ProviderConfig{}
	if err := c.kube.Get(ctx, types.NamespacedName{Name: cr.GetProviderConfigReference().Name}, pc); err != nil {
		return nil, errors.Wrap(err, errGetPC)
	}

	cd := pc.Spec.Credentials
	data, err := resource.CommonCredentialExtractor(ctx, cd.Source, c.kube, cd.CommonCredentialSelectors)
	if err != nil {
		return nil, errors.Wrap(err, errGetCreds)
	}

	svc, err := c.newServiceFn(ctx, data, c.kube)
	if err != nil {
		return nil, errors.Wrap(err, errNewClient)
	}
	c.cachedClient = svc

	return &external{kafkaClient: svc, log: c.log}, nil
}

func (c *connectDisconnector) Disconnect(ctx context.Context) error {
	if c.cachedClient != nil {
		c.cachedClient.Close()
	}
	c.cachedClient = nil
	return nil
}

// An ExternalClient observes, then either creates, updates, or deletes an
// external resource to ensure it reflects the managed resource's desired state.
type external struct {
	kafkaClient *kadm.Client
	log         logging.Logger
}

func (c *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	cr, ok := mg.(*v1alpha1.User)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotUser)
	}

	u, err := user.Get(ctx, c.kafkaClient, meta.GetExternalName(cr))
	if err != nil { // Discern whether the user doesn't exist or something went wrong
		if strings.HasPrefix(err.Error(), user.ErrUserDoesNotExist) {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrapf(err, errGetUser)
	}

	cr.Status.SetConditions(v1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        user.IsUpToDate(&cr.Spec.ForProvider, u),
		ResourceLateInitialized: false,
	}, nil
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*v1alpha1.User)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotUser)
	}
	password, err := password.Generate()
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errGeneratePassword)
	}
	err = user.Create(ctx, c.kafkaClient, user.Generate(meta.GetExternalName(cr), &cr.Spec.ForProvider), password)
	if err != nil {
		return managed.ExternalCreation{}, err
	}
	cd := managed.ConnectionDetails{
		v1.ResourceCredentialsSecretPasswordKey: []byte(password),
	}
	return managed.ExternalCreation{
		ConnectionDetails: cd,
	}, nil
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	return managed.ExternalUpdate{}, errors.New(errUpdateNotSupported)
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) error {
	cr, ok := mg.(*v1alpha1.User)
	if !ok {
		return errors.New(errNotUser)
	}
	return user.Delete(ctx, c.kafkaClient, user.Generate(meta.GetExternalName(cr), &cr.Spec.ForProvider))
}
