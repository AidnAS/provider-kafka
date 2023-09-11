package user

import (
	"context"

	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/crossplane-contrib/provider-kafka/apis/user/v1alpha1"
)

// User is a holistic representation of a Kafka user with all configurable
// fields
type User struct {
	Name       string
	Mechanism  kadm.ScramMechanism
	Iterations int32
}

const (
	errCannotDescribeUser       = "cannot describe user"
	errNoCreateResponseForUser  = "no create response for user"
	errCannotCreateUser         = "cannot create user"
	errNoDeleteResponseForUser  = "no delete response for user"
	errCannotDeleteUser         = "cannot delete user"
	errMultipleCredentialsFound = "user has multiple credentials"
	errUserAlreadyExists        = "user already exists"

	// ErrUserDoesNotExist indicates that the user of a given name doesn't exist in the external Kafka cluster
	ErrUserDoesNotExist = "user does not exist"
)

// Get gets the user from Kafka side and returns a User object.
func Get(ctx context.Context, client *kadm.Client, name string) (*User, error) {
	ud, err := client.DescribeUserSCRAMs(ctx, name)
	if err != nil {
		return nil, errors.Wrap(err, errCannotDescribeUser)
	}
	if ud[name].Err != nil {
		return nil, errors.Wrap(ud[name].Err, ErrUserDoesNotExist)
	}

	u := ud[name]

	user := User{}
	user.Name = name
	if len(u.CredInfos) != 1 {
		return nil, errors.New(errMultipleCredentialsFound)
	}
	user.Mechanism = u.CredInfos[0].Mechanism
	user.Iterations = u.CredInfos[0].Iterations
	return &user, nil
}

// Create creates the user from the Kafka side and returns the password
func Create(ctx context.Context, client *kadm.Client, user *User, password string) error {
	ud, err := client.DescribeUserSCRAMs(ctx, user.Name)
	if err != nil {
		return errors.Wrap(err, errCannotDescribeUser)
	}
	if ud[user.Name].Err == nil {
		return errors.New(errUserAlreadyExists)
	}
	upsert := kadm.UpsertSCRAM{
		User:       user.Name,
		Mechanism:  user.Mechanism,
		Iterations: user.Iterations,
		Password:   password,
	}

	resp, err := client.AlterUserSCRAMs(ctx, []kadm.DeleteSCRAM{}, []kadm.UpsertSCRAM{upsert})
	if err != nil {
		return err
	}

	u, ok := resp[user.Name]
	if !ok {
		return errors.New(errNoCreateResponseForUser)
	}
	if u.Err != nil {
		return errors.Wrap(u.Err, errCannotCreateUser)
	}
	return nil
}

// Delete deletes the user from Kafka side
func Delete(ctx context.Context, client *kadm.Client, user *User) error {
	delete := kadm.DeleteSCRAM{
		User:      user.Name,
		Mechanism: user.Mechanism,
	}
	resp, err := client.AlterUserSCRAMs(ctx, []kadm.DeleteSCRAM{delete}, []kadm.UpsertSCRAM{})
	if err != nil {
		return err
	}

	u, ok := resp[user.Name]
	if !ok {
		return errors.New(errNoDeleteResponseForUser)
	}
	if u.Err != nil {
		return errors.Wrap(u.Err, errCannotDeleteUser)
	}
	return nil
}

// Generate is used to convert Crossplane UserParameters to Kafka User.
func Generate(name string, params *v1alpha1.UserParameters) *User {
	scramMap := map[string]kadm.ScramMechanism{
		"SCRAM-SHA-256": kadm.ScramSha256,
		"SCRAM-SHA-512": kadm.ScramSha512,
	}

	user := &User{
		Name:       name,
		Mechanism:  scramMap[params.Mechanism],
		Iterations: int32(params.Iterations),
	}
	return user
}

// IsUpToDate returns true if the supplied Kubernetes resource differs from the
// supplied User.
func IsUpToDate(in *v1alpha1.UserParameters, observed *User) bool {
	if in.Mechanism != observed.Mechanism.String() {
		return false
	}
	if in.Iterations != int(observed.Iterations) {
		return false
	}
	return true
}
