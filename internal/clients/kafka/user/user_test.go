package user

import (
	"context"
	"fmt"

	"os"
	"testing"

	"github.com/crossplane-contrib/provider-kafka/apis/user/v1alpha1"
	"github.com/crossplane-contrib/provider-kafka/internal/clients/kafka"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kadm"
)

var kafkaPassword = os.Getenv("KAFKA_PASSWORD")

var dataTesting = []byte(
	fmt.Sprintf(`{
			"brokers": [ "kafka-dev-0.kafka-dev-headless:9092"],
			"sasl": {
				"mechanism": "PLAIN",
				"username": "user",
				"password": "%s"
			}
		}`, kafkaPassword),
)

func TestCreate(t *testing.T) {

	newAc, _ := kafka.NewAdminClient(context.Background(), dataTesting, nil)

	type args struct {
		ctx      context.Context
		client   *kadm.Client
		user     *User
		password string
	}
	{
		cases := map[string]struct {
			name    string
			args    args
			wantErr bool
		}{
			"CreateUserOne": {
				name: "CreateUserOne",
				args: args{
					ctx:    context.Background(),
					client: newAc,
					user: &User{
						Name:       "testUser-1",
						Mechanism:  kadm.ScramSha512,
						Iterations: 4096,
					},
					password: "password1",
				},
				wantErr: false,
			},
			"CreateUserTwo": {
				name: "CreateUserTwo",
				args: args{
					ctx:    context.Background(),
					client: newAc,
					user: &User{
						Name:       "testUser-2",
						Mechanism:  kadm.ScramSha512,
						Iterations: 4096,
					},
					password: "password2",
				},
				wantErr: false,
			},
		}
		for _, tt := range cases {
			t.Run(tt.name, func(t *testing.T) {
				if err := Create(tt.args.ctx, tt.args.client, tt.args.user, tt.args.password); (err != nil) != tt.wantErr {
					t.Errorf("Create() error = %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	}
}

func TestGet(t *testing.T) {

	newAc, _ := kafka.NewAdminClient(context.Background(), dataTesting, nil)

	type args struct {
		ctx    context.Context
		client *kadm.Client
		name   string
	}
	cases := map[string]struct {
		name    string
		args    args
		want    *User
		wantErr bool
	}{
		"GetUserWorked": {
			name: "GetUserWorked",
			args: args{
				ctx:    context.Background(),
				client: newAc,
				name:   "testUser-1",
			},
			want: &User{
				Name:       "testUser-1",
				Mechanism:  kadm.ScramSha512,
				Iterations: 4096,
			},
			wantErr: false,
		},
		"GetUserDoesNotExist": {
			name: "GetUserDoesNotExist",
			args: args{
				ctx:    context.Background(),
				client: newAc,
				name:   "testUser-00",
			},
			want: &User{
				Name:       "testUser-1",
				Mechanism:  kadm.ScramSha512,
				Iterations: 4096,
			},
			wantErr: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Get(tt.args.ctx, tt.args.client, tt.args.name); (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGenerate(t *testing.T) {
	type args struct {
		name   string
		params *v1alpha1.UserParameters
	}

	type want struct {
		user *User
	}

	cases := map[string]struct {
		args args
		want want
	}{
		"ValidComparison": {
			args: args{
				name: "validComparison",
				params: &v1alpha1.UserParameters{
					Mechanism:  "SCRAM-SHA-512",
					Iterations: 4096,
				},
			},
			want: want{
				&User{
					Name:       "validComparison",
					Mechanism:  kadm.ScramSha512,
					Iterations: 4096,
				},
			},
		},
	}

	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			user := Generate(tt.args.name, tt.args.params)
			if diff := cmp.Diff(tt.want.user, user); diff != "" {
				t.Errorf("Generate() =  -want, +got:\n%s", diff)
			}
		})
	}
}

func TestIsUpToDate(t *testing.T) {
	type args struct {
		in       *v1alpha1.UserParameters
		observed *User
	}

	cases := map[string]struct {
		name string
		args args
		want bool
	}{
		"IsUpToDate": {
			name: "upToDate",
			args: args{
				in: &v1alpha1.UserParameters{
					Mechanism:  "SCRAM-SHA-512",
					Iterations: 4096,
				},
				observed: &User{
					Name:       "upToDate",
					Mechanism:  kadm.ScramSha512,
					Iterations: 4096,
				},
			},
			want: true,
		},
		"DiffMechanism": {
			name: "mechanismDiff",
			args: args{
				in: &v1alpha1.UserParameters{
					Mechanism:  "SCRAM-SHA-512",
					Iterations: 4096,
				},
				observed: &User{
					Name:       "mechanismDiff",
					Mechanism:  kadm.ScramSha256,
					Iterations: 4096,
				},
			},
			want: false,
		},
	}
	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			isUpToDate := IsUpToDate(tt.args.in, tt.args.observed)
			if diff := cmp.Diff(tt.want, isUpToDate); diff != "" {
				t.Errorf("IsUpToDate() = -want +got")
			}

		})
	}
}

func TestCreateDuplicateUser(t *testing.T) {

	newAc, _ := kafka.NewAdminClient(context.Background(), dataTesting, nil)

	type args struct {
		ctx      context.Context
		client   *kadm.Client
		user     *User
		password string
	}
	{
		cases := map[string]struct {
			name    string
			args    args
			wantErr bool
		}{
			"CreateUserOneExists": {
				name: "CreateUserOneExists",
				args: args{
					ctx:    context.Background(),
					client: newAc,
					user: &User{
						Name:       "testUser-1",
						Mechanism:  kadm.ScramSha512,
						Iterations: 4096,
					},
					password: "password1",
				},
				wantErr: true,
			},

			"CreateUserTwoExists": {
				name: "CreateTwoExists",
				args: args{
					ctx:    context.Background(),
					client: newAc,
					user: &User{
						Name:       "testUser-2",
						Mechanism:  kadm.ScramSha512,
						Iterations: 4096,
					},
					password: "password2",
				},
				wantErr: true,
			},

			"CreateUserDoesNotExist": {
				name: "CreateUserThreeDoesNotExist",
				args: args{
					ctx:    context.Background(),
					client: newAc,
					user: &User{
						Name:       "testUser-3",
						Mechanism:  kadm.ScramSha512,
						Iterations: 4096,
					},
					password: "password3",
				},
				wantErr: false,
			},
		}

		for _, tt := range cases {
			t.Run(tt.name, func(t *testing.T) {
				if err := Create(tt.args.ctx, tt.args.client, tt.args.user, tt.args.password); (err != nil) != tt.wantErr {
					t.Errorf("Create() error = %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	}
}

func TestDelete(t *testing.T) {

	newAc, _ := kafka.NewAdminClient(context.Background(), dataTesting, nil)

	type args struct {
		ctx    context.Context
		client *kadm.Client
		user   *User
	}
	cases := map[string]struct {
		name    string
		args    args
		wantErr bool
	}{
		"DeleteUserOne": {
			name: "DeleteUserOne",
			args: args{
				ctx:    context.Background(),
				client: newAc,
				user: &User{
					Name:       "testUser-1",
					Mechanism:  kadm.ScramSha512,
					Iterations: 4096,
				},
			},
			wantErr: false,
		},
		"DeleteUserTwo": {
			name: "DeleteUserTwo",
			args: args{
				ctx:    context.Background(),
				client: newAc,
				user: &User{
					Name:       "testUser-2",
					Mechanism:  kadm.ScramSha512,
					Iterations: 4096,
				},
			},
			wantErr: false,
		},
		"DeleteUserThree": {
			name: "DeleteUserThree",
			args: args{
				ctx:    context.Background(),
				client: newAc,
				user: &User{
					Name:       "testUser-3",
					Mechanism:  kadm.ScramSha512,
					Iterations: 4096,
				},
			},
			wantErr: false,
		},
		"DeleteUserFourDoesNotExist": {
			name: "DeleteUserFourDoesNotExist",
			args: args{
				ctx:    context.Background(),
				client: newAc,
				user: &User{
					Name:       "testUser-4",
					Mechanism:  kadm.ScramSha512,
					Iterations: 4096,
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if err := Delete(tt.args.ctx, tt.args.client, tt.args.user); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
