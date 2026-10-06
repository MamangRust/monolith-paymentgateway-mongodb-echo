package auth_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	authhandler "github.com/MamangRust/monolith-payment-gateway-apigateway/handler/auth"
	"github.com/MamangRust/monolith-payment-gateway-auth/handler"
	"github.com/MamangRust/monolith-payment-gateway-auth/repository"
	"github.com/MamangRust/monolith-payment-gateway-auth/service"
	pb "github.com/MamangRust/monolith-payment-gateway-pb"
	pbrole "github.com/MamangRust/monolith-payment-gateway-pb/role"
	pbuserrole "github.com/MamangRust/monolith-payment-gateway-pb/user_role"
	"github.com/MamangRust/monolith-payment-gateway-pkg/auth"
	"github.com/MamangRust/monolith-payment-gateway-pkg/hash"
	"github.com/MamangRust/monolith-payment-gateway-pkg/logger"
	role_handler "github.com/MamangRust/monolith-payment-gateway-role/handler"
	role_repository "github.com/MamangRust/monolith-payment-gateway-role/repository"
	role_service "github.com/MamangRust/monolith-payment-gateway-role/service"
	"github.com/MamangRust/monolith-payment-gateway-shared/cache"
	apierror "github.com/MamangRust/monolith-payment-gateway-shared/errors"
	"github.com/MamangRust/monolith-payment-gateway-shared/observability"
	tests "github.com/MamangRust/monolith-payment-gateway-test"
	"github.com/labstack/echo/v4"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type AuthHandlerApiTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	redisClient *redis.Client
	server      *echo.Echo
	email       string
	password    string
	accessToken string
	userID      int
}

func (s *AuthHandlerApiTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts

	mongoDB, err := s.ts.MongoDB()
	if err != nil {
		s.Require().NoError(err)
	}
	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opts)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(s.redisClient, log, cacheMetrics)

	// Create role service
	roleRepos := role_repository.NewRepositories(mongoDB)
	roleSvc := role_service.NewService(&role_service.Deps{
		Cache:        cacheStore,
		Repositories: roleRepos,
		Logger:       log,
	})
	roleH := role_handler.NewHandler(roleSvc)

	// Create bufconn-based gRPC server with ALL services registered BEFORE serving
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()

	// Register role service (needed by auth for role lookups)
	pbrole.RegisterRoleQueryServiceServer(grpcServer, roleH.RoleQuery)
	pbrole.RegisterRoleCommandServiceServer(grpcServer, roleH.RoleCommand)
	pbuserrole.RegisterUserRoleServiceServer(grpcServer, roleH.UserRole)

	// Create role query client from same bufconn
	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)

	roleQueryClient := pbrole.NewRoleQueryServiceClient(conn)

	// Create auth service with role query client
	repos := repository.NewRepositories(&repository.RepositoriesDeps{
		DB:              mongoDB,
		RoleQueryClient: roleQueryClient,
		UserRoleClient:  pbuserrole.NewUserRoleServiceClient(conn),
	})

	tokenManager, _ := auth.NewManager("mysecret")
	hasher := hash.NewHashingPassword()

	svc := service.NewService(&service.Deps{
		Repositories: repos,
		Logger:       log,
		Cache:        cacheStore,
		Token:        tokenManager,
		Hash:         hasher,
		Kafka:        nil,
	})

	h := handler.NewAuthHandleGrpc(svc, log)
	pb.RegisterAuthServiceServer(grpcServer, h)

	// Start serving AFTER all services are registered
	go func() { _ = grpcServer.Serve(lis) }()

	s.server = echo.New()

	obs, _ := observability.NewObservability("test", log)
	apiHandler := apierror.NewApiHandler(obs, log)

	// Auth bypass middleware for /api/auth/me
	s.server.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if s.userID != 0 {
				c.Set("userId", strconv.Itoa(s.userID))
			}
			return next(c)
		}
	})

	authhandler.RegisterAuthHandler(&authhandler.DepsAuth{
		Client:     conn,
		E:          s.server,
		Logger:     log,
		Cache:      cacheStore,
		ApiHandler: apiHandler,
	})

	s.email = "auth.handler.api.test@example.com"
	s.password = "password123"

	// Seed ROLE_ADMIN
	now := time.Now()
	if _, err = mongoDB.Collection("roles").InsertOne(context.Background(), bson.M{
		"role_id":    1,
		"role_name":  "ROLE_ADMIN",
		"created_at": &now,
		"updated_at": &now,
	}); err != nil {
		s.Require().NoError(err)
	}
}

func (s *AuthHandlerApiTestSuite) TearDownSuite() {
	s.ts.Teardown()
}

func (s *AuthHandlerApiTestSuite) Test1_Register() {
	body := map[string]string{
		"email":            s.email,
		"password":         s.password,
		"confirm_password": s.password,
		"firstname":        "Auth",
		"lastname":         "Test",
	}
	jsonBody, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/api/auth/register", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.server.ServeHTTP(rec, req)
	s.Require().Equal(http.StatusCreated, rec.Code, rec.Body.String())

	var res map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &res)
	data := res["data"].(map[string]interface{})
	s.userID = int(data["id"].(float64))

	// Verify the user so login can proceed
	mongoDB, _ := s.ts.MongoDB()
	_, _ = mongoDB.Collection("users").UpdateOne(context.Background(),
		bson.M{"user_id": int32(s.userID)},
		bson.M{"$set": bson.M{"is_verified": true}})
}

func (s *AuthHandlerApiTestSuite) Test2_Login() {
	body := map[string]string{
		"email":    s.email,
		"password": s.password,
	}
	jsonBody, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/api/auth/login", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.server.ServeHTTP(rec, req)
	s.Require().Equal(http.StatusOK, rec.Code, rec.Body.String())

	var res map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &res)
	data := res["data"].(map[string]interface{})
	s.accessToken = data["access_token"].(string)
}

func TestAuthHandlerApiSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(AuthHandlerApiTestSuite))
}
