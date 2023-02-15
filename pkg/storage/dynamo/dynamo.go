package dynamo

import (

	"time"
	"fmt"
	"context"
	"sync"
	"sort"
	"strconv"
	"strings"

    "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/openfga/openfga/pkg/logger"
	"go.opentelemetry.io/otel"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/common"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/dynamodb"
	"google.golang.org/protobuf/types/known/timestamppb"
	// sq "github.com/Masterminds/squirrel"
	"github.com/openfga/openfga/pkg/telemetry"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

var tracer = otel.Tracer("openfga/pkg/storage/dynamo")

type Dynamo struct {
	db                     dynamodbiface.DynamoDBAPI
	logger                 logger.Logger
	maxTuplesPerWriteField int
	maxTypesPerModelField  int
	maxTuplesPerWrite             int
	maxTypesPerAuthorizationModel int
	stores map[string]*openfgapb.Store
	assertions map[string][]*openfgapb.Assertion
	mu                            sync.Mutex
	tuples map[string][]*openfgapb.Tuple /* GUARDED_BY(mu) */

	// ChangelogBackend
	// map: store => set of changes
	changes map[string][]*openfgapb.TupleChange
	authorizationModels map[string]map[string]*AuthorizationModelEntry /* GUARDED_BY(mu_) */
}

type AuthorizationModelEntry struct {
	model  *openfgapb.AuthorizationModel
	latest bool
}

var _ storage.OpenFGADatastore = (*Dynamo)(nil)

func New(uri string, cfg *common.Config) (*Dynamo, error) {

	sess, err := session.NewSession(&aws.Config{     
    		Endpoint: aws.String("http://localhost:8000")}, 
	)
	
	if err != nil {
        	fmt.Errorf("Got error creating session: %s", err)
			//cfg.Logger.Info
    }
		
	db := dynamodb.New(sess)

	return &Dynamo{
		// stbl:                   sq.StatementBuilder.RunWith(db),
		db:                     db,
		logger:                 cfg.Logger,
		maxTuplesPerWriteField: cfg.MaxTuplesPerWriteField,
		maxTypesPerModelField:  cfg.MaxTypesPerModelField,
	}, nil
}

// Close closes the datastore and cleans up any residual resources.
func (m *Dynamo) Close() {
	// m.db.Close()
}

// CreateStore is slightly different between Postgres and MySQL
func (m *Dynamo) CreateStore(ctx context.Context, store *openfgapb.Store) (*openfgapb.Store, error) {
	// ctx, span := tracer.Start(ctx, "dynamo.CreateStore")

	var createdAt time.Time
	id, name := "id", "dynamo"

	return &openfgapb.Store{
		Id:        id,
		Name:      name,
		CreatedAt: timestamppb.New(createdAt),
		UpdatedAt: timestamppb.New(createdAt),
	}, nil
}

func (m *Dynamo) DeleteStore(ctx context.Context, id string) error {
	// ctx, span := tracer.Start(ctx, "dynamo.DeleteStore")

	// _, err := m.stbl.
	// 	Update("store").
	// 	Set("deleted_at", sq.Expr("NOW()")).
	// 	Where(sq.Eq{"id": id}).
	// 	ExecContext(ctx)
	// if err != nil {
	// 	return common.HandleSQLError(err)
	// }

	return nil
}

func (m *Dynamo) FindLatestAuthorizationModelID(ctx context.Context, store string) (string, error) {
	// ctx, span := tracer.Start(ctx, "mysql.FindLatestAuthorizationModelID")
	// defer span.End()

	var modelID = "modelID"
	// err := m.stbl.
	// 	Select("authorization_model_id").
	// 	From("authorization_model").
	// 	Where(sq.Eq{"store": store}).
	// 	OrderBy("authorization_model_id desc").
	// 	Limit(1).
	// 	QueryRowContext(ctx).
	// 	Scan(&modelID)
	// if err != nil {
	// 	return "", common.HandleSQLError(err)
	// }

	return modelID, nil
}

func (m *Dynamo) GetStore(ctx context.Context, id string) (*openfgapb.Store, error) {

	var createdAt time.Time
	storeID, name := "storeId", "name"

	return &openfgapb.Store{
		Id:        storeID,
		Name:      name,
		CreatedAt: timestamppb.New(createdAt),
		UpdatedAt: timestamppb.New(createdAt),
	}, nil
}

func (m *Dynamo) IsReady(ctx context.Context) (bool, error) {
	// ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	// defer cancel()

	// if err := m.db.PingContext(ctx); err != nil {
	// 	return false, err
	// }

	return true, nil
}

func (m *Dynamo) ListObjectsByType(ctx context.Context, store string, objectType string) (storage.ObjectIterator, error) {
	// ctx, span := tracer.Start(ctx, "mysql.ListObjectsByType")
	// defer span.End()

	// rows, err := m.stbl.
	// 	Select("object_type", "object_id").
	// 	Distinct().
	// 	From("tuple").
	// 	Where(sq.Eq{
	// 		"store":       store,
	// 		"object_type": objectType,
	// 	}).
	// 	QueryContext(ctx)
	// if err != nil {
	// 	return nil, common.HandleSQLError(err)
	// }

	matches := make([]*openfgapb.Object, 0)

	return storage.NewStaticObjectIterator(matches), nil
}


func (s *Dynamo) ListStores(ctx context.Context, paginationOptions storage.PaginationOptions) ([]*openfgapb.Store, []byte, error) {
	// _, span := tracer.Start(ctx, "memory.ListStores")
	// defer span.End()

	// s.mu.Lock()
	// defer s.mu.Unlock()

	stores := make([]*openfgapb.Store, 0, len(s.stores))

	// var err error
	var from int64 = 0
	pageSize := storage.DefaultPageSize

	to := int(from) + pageSize
	continuationToken := ""
	res := stores[from:to]

	return res, []byte(continuationToken), nil
}

func (s *Dynamo) MaxTuplesPerWrite() int {
	return s.maxTuplesPerWrite
}


// MaxTypesPerAuthorizationModel returns the maximum number of types allowed in a type definition
func (s *Dynamo) MaxTypesPerAuthorizationModel() int {
	return s.maxTypesPerAuthorizationModel
}

func (s *Dynamo) Read(ctx context.Context, store string, key *openfgapb.TupleKey) (storage.TupleIterator, error) {
	ctx, span := tracer.Start(ctx, "memory.Read")
	defer span.End()

	return s.read(ctx, store, key, storage.PaginationOptions{})
}

type staticIterator struct {
	tuples            []*openfgapb.Tuple
	continuationToken []byte
}

func (s *Dynamo) read(ctx context.Context, store string, tk *openfgapb.TupleKey, paginationOptions storage.PaginationOptions) (*staticIterator, error) {
	// _, span := tracer.Start(ctx, "memory.read")
	// defer span.End()

	// s.mu.Lock()
	// defer s.mu.Unlock()

	var matches []*openfgapb.Tuple

	return &staticIterator{tuples: matches}, nil
}

func (s *Dynamo) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgapb.Assertion, error) {
	_, span := tracer.Start(ctx, "memory.ReadAssertions")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	assertionsID := fmt.Sprintf("%s|%s", store, modelID)
	assertions, ok := s.assertions[assertionsID]
	if !ok {
		return []*openfgapb.Assertion{}, nil
	}
	return assertions, nil
}

// ReadAuthorizationModel See storage.AuthorizationModelBackend.ReadAuthorizationModel
func (s *Dynamo) ReadAuthorizationModel(ctx context.Context, store string, id string) (*openfgapb.AuthorizationModel, error) {
	_, span := tracer.Start(ctx, "memory.ReadAuthorizationModel")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	tm, ok := s.authorizationModels[store]
	if !ok {
		telemetry.TraceError(span, storage.ErrNotFound)
		return nil, storage.ErrNotFound
	}

	if model, ok := findAuthorizationModelByID(id, tm); ok {
		if model.GetTypeDefinitions() == nil || len(model.GetTypeDefinitions()) == 0 {
			return nil, storage.ErrNotFound
		}
		return model, nil
	}

	telemetry.TraceError(span, storage.ErrNotFound)
	return nil, storage.ErrNotFound
}

func findAuthorizationModelByID(id string, configurations map[string]*AuthorizationModelEntry) (*openfgapb.AuthorizationModel, bool) {
	var nsc *openfgapb.AuthorizationModel
	return nsc, true
}

func (s *Dynamo) ReadAuthorizationModels(ctx context.Context, store string, options storage.PaginationOptions) ([]*openfgapb.AuthorizationModel, []byte, error) {
	_, span := tracer.Start(ctx, "memory.ReadAuthorizationModels")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	models := make([]*openfgapb.AuthorizationModel, 0, len(s.authorizationModels[store]))
	for _, entry := range s.authorizationModels[store] {
		models = append(models, entry.model)
	}

	// from newest to oldest
	sort.Slice(models, func(i, j int) bool {
		return models[i].Id > models[j].Id
	})

	var from int64 = 0
	continuationToken := ""
	var err error

	pageSize := storage.DefaultPageSize
	if options.PageSize > 0 {
		pageSize = options.PageSize
	}

	if options.From != "" {
		from, err = strconv.ParseInt(options.From, 10, 32)
		if err != nil {
			return nil, nil, err
		}
	}

	to := int(from) + pageSize
	if len(models) < to {
		to = len(models)
	}
	res := models[from:to]

	if to != len(models) {
		continuationToken = strconv.Itoa(to)
	}

	return res, []byte(continuationToken), nil
}

func (s *Dynamo) ReadChanges(ctx context.Context, store, objectType string, paginationOptions storage.PaginationOptions, horizonOffset time.Duration) ([]*openfgapb.TupleChange, []byte, error) {
	_, span := tracer.Start(ctx, "memory.ReadChanges")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	var err error
	var from int64
	var typeInToken string
	var continuationToken string
	if paginationOptions.From != "" {
		tokens := strings.Split(paginationOptions.From, "|")
		if len(tokens) == 2 {
			concreteToken := tokens[0]
			typeInToken = tokens[1]
			from, err = strconv.ParseInt(concreteToken, 10, 32)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	if typeInToken != "" && typeInToken != objectType {
		return nil, nil, storage.ErrMismatchObjectType
	}

	var allChanges []*openfgapb.TupleChange
	now := time.Now().UTC()
	for _, change := range s.changes[store] {
		if objectType == "" || (objectType != "" && strings.HasPrefix(change.TupleKey.Object, objectType+":")) {
			if change.Timestamp.AsTime().After(now.Add(-horizonOffset)) {
				break
			}
			allChanges = append(allChanges, change)
		}
	}
	if len(allChanges) == 0 {
		return nil, nil, storage.ErrNotFound
	}

	pageSize := storage.DefaultPageSize
	if paginationOptions.PageSize > 0 {
		pageSize = paginationOptions.PageSize
	}
	to := int(from) + pageSize
	if len(allChanges) < to {
		to = len(allChanges)
	}
	res := allChanges[from:to]
	if len(res) == 0 {
		return nil, nil, storage.ErrNotFound
	}

	continuationToken = strconv.Itoa(len(allChanges))
	if to != len(allChanges) {
		continuationToken = strconv.Itoa(to)
	}
	continuationToken = continuationToken + fmt.Sprintf("|%s", objectType)

	return res, []byte(continuationToken), nil
}


func (s *Dynamo) ReadPage(ctx context.Context, store string, key *openfgapb.TupleKey, paginationOptions storage.PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	ctx, span := tracer.Start(ctx, "memory.ReadPage")
	defer span.End()

	it, err := s.read(ctx, store, key, paginationOptions)
	if err != nil {
		return nil, nil, err
	}

	return it.tuples, it.continuationToken, nil
}

func (s *Dynamo) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter storage.ReadStartingWithUserFilter,
) (storage.TupleIterator, error) {
	_, span := tracer.Start(ctx, "memory.ReadStartingWithUser")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	var matches []*openfgapb.Tuple
	for _, t := range s.tuples[store] {
		if tupleUtils.GetType(t.Key.GetObject()) != filter.ObjectType {
			continue
		}

		if t.Key.GetRelation() != filter.Relation {
			continue
		}

		for _, userFilter := range filter.UserFilter {
			targetUser := userFilter.GetObject()
			if userFilter.GetRelation() != "" {
				targetUser = tupleUtils.GetObjectRelationAsString(userFilter)
			}

			if targetUser == t.Key.GetUser() {
				matches = append(matches, t)
			}
		}

	}
	return &staticIterator{tuples: matches}, nil
}

// ReadTypeDefinition See storage.TypeDefinitionReadBackend.ReadTypeDefinition
func (s *Dynamo) ReadTypeDefinition(ctx context.Context, store, id, objectType string) (*openfgapb.TypeDefinition, error) {
	_, span := tracer.Start(ctx, "memory.ReadTypeDefinition")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	tm, ok := s.authorizationModels[store]
	if !ok {
		telemetry.TraceError(span, storage.ErrNotFound)
		return nil, storage.ErrNotFound
	}

	if nsc, ok := findAuthorizationModelByID(id, tm); ok {
		if ns, ok := definitionByType(nsc, objectType); ok {
			return ns, nil
		}
	}

	telemetry.TraceError(span, storage.ErrNotFound)
	return nil, storage.ErrNotFound
}

// ReadUserTuple See storage.TupleBackend.ReadUserTuple
func (s *Dynamo) ReadUserTuple(ctx context.Context, store string, key *openfgapb.TupleKey) (*openfgapb.Tuple, error) {
	_, span := tracer.Start(ctx, "memory.ReadUserTuple")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, t := range s.tuples[store] {
		if match(key, t.Key) {
			return t, nil
		}
	}

	telemetry.TraceError(span, storage.ErrNotFound)
	return nil, storage.ErrNotFound
}

// definitionByType returns the definition of the objectType if it exists in the authorization model
func definitionByType(authorizationModel *openfgapb.AuthorizationModel, objectType string) (*openfgapb.TypeDefinition, bool) {
	for _, td := range authorizationModel.GetTypeDefinitions() {
		if td.GetType() == objectType {
			return td, true
		}
	}

	return nil, false
}

func match(key *openfgapb.TupleKey, target *openfgapb.TupleKey) bool {
	if key.Object != "" {
		td, objectid := tupleUtils.SplitObject(key.Object)
		if objectid == "" {
			if td != tupleUtils.GetType(target.Object) {
				return false
			}
		} else {
			if key.Object != target.Object {
				return false
			}
		}
	}
	if key.Relation != "" && key.Relation != target.Relation {
		return false
	}
	if key.User != "" && key.User != target.User {
		return false
	}
	return true
}


// ReadUsersetTuples See storage.TupleBackend.ReadUsersetTuples
func (s *Dynamo) ReadUsersetTuples(ctx context.Context, store string, tk *openfgapb.TupleKey) (storage.TupleIterator, error) {
	_, span := tracer.Start(ctx, "memory.ReadUsersetTuples")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	var matches []*openfgapb.Tuple
	for _, t := range s.tuples[store] {
		if match(&openfgapb.TupleKey{
			Object:   tk.GetObject(),
			Relation: tk.GetRelation(),
		}, t.Key) && tupleUtils.GetUserTypeFromUser(t.GetKey().GetUser()) == tupleUtils.UserSet {
			matches = append(matches, t)
		}
	}

	return &staticIterator{tuples: matches}, nil
}

// Write See storage.TupleBackend.Write
func (s *Dynamo) Write(ctx context.Context, store string, deletes storage.Deletes, writes storage.Writes) error {
	_, span := tracer.Start(ctx, "memory.Write")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	now := timestamppb.Now()

	if err := validateTuples(s.tuples[store], deletes, writes); err != nil {
		return err
	}

	var tuples []*openfgapb.Tuple
Delete:
	for _, t := range s.tuples[store] {
		for _, k := range deletes {
			if match(k, t.Key) {
				s.changes[store] = append(s.changes[store], &openfgapb.TupleChange{TupleKey: t.Key, Operation: openfgapb.TupleOperation_TUPLE_OPERATION_DELETE, Timestamp: now})
				continue Delete
			}
		}
		tuples = append(tuples, t)
	}

Write:
	for _, t := range writes {
		for _, et := range tuples {
			if match(t, et.Key) {
				continue Write
			}
		}
		tuples = append(tuples, &openfgapb.Tuple{Key: t, Timestamp: now})
		s.changes[store] = append(s.changes[store], &openfgapb.TupleChange{TupleKey: t, Operation: openfgapb.TupleOperation_TUPLE_OPERATION_WRITE, Timestamp: now})
	}
	s.tuples[store] = tuples
	return nil
}

func (s *Dynamo) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgapb.Assertion) error {
	_, span := tracer.Start(ctx, "memory.WriteAssertions")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	assertionsID := fmt.Sprintf("%s|%s", store, modelID)
	s.assertions[assertionsID] = assertions

	return nil
}

func (s *Dynamo) WriteAuthorizationModel(ctx context.Context, store string, model *openfgapb.AuthorizationModel) error {
	_, span := tracer.Start(ctx, "memory.WriteAuthorizationModel")
	defer span.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.authorizationModels[store]; !ok {
		s.authorizationModels[store] = make(map[string]*AuthorizationModelEntry)
	}

	for _, entry := range s.authorizationModels[store] {
		entry.latest = false
	}

	s.authorizationModels[store][model.Id] = &AuthorizationModelEntry{
		model:  model,
		latest: true,
	}

	return nil
}

func validateTuples(tuples []*openfgapb.Tuple, deletes, writes []*openfgapb.TupleKey) error {
	for _, tk := range deletes {
		if !find(tuples, tk) {
			return storage.InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_DELETE)
		}
	}
	for _, tk := range writes {
		if find(tuples, tk) {
			return storage.InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_WRITE)
		}
	}
	return nil
}

func find(tuples []*openfgapb.Tuple, tupleKey *openfgapb.TupleKey) bool {
	for _, tuple := range tuples {
		if match(tuple.Key, tupleKey) {
			return true
		}
	}
	return false
}

func (s *staticIterator) Next() (*openfgapb.Tuple, error) {
	if len(s.tuples) == 0 {
		return nil, storage.ErrIteratorDone
	}

	next, rest := s.tuples[0], s.tuples[1:]
	s.tuples = rest

	return next, nil
}

func (s *staticIterator) Stop() {}
