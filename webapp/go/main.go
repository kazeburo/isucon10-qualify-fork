package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"encoding/json"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
	"go.opencensus.io/plugin/ochttp"
	"golang.org/x/sync/singleflight"
)

const Limit = 20
const NazotteLimit = 50

var db *sqlx.DB
var mySQLConnectionData *MySQLConnectionEnv
var chairSearchCondition ChairSearchCondition
var estateSearchCondition EstateSearchCondition
var chairSearchConditionJSON []byte
var estateSearchConditionJSON []byte

var chairCache = NewSC()
var chairObjCache = NewCC()
var estateCache = NewSC()
var estateObjCache = NewIC()
var sfGroup singleflight.Group

type InitializeResponse struct {
	Language string `json:"language"`
}

type Chair struct {
	ID          int64  `db:"id" json:"id"`
	Name        string `db:"name" json:"name"`
	Description string `db:"description" json:"description"`
	Thumbnail   string `db:"thumbnail" json:"thumbnail"`
	Price       int64  `db:"price" json:"price"`
	Height      int64  `db:"height" json:"height"`
	Width       int64  `db:"width" json:"width"`
	Depth       int64  `db:"depth" json:"depth"`
	Color       string `db:"color" json:"color"`
	Features    string `db:"features" json:"features"`
	Kind        string `db:"kind" json:"kind"`
	Popularity  int64  `db:"popularity" json:"-"`
	Stock       int64  `db:"stock" json:"-"`
	PriceRange  int64  `db:"price_range" json:"-"`
	HeightRange int64  `db:"height_range" json:"-"`
	WidthRange  int64  `db:"width_range" json:"-"`
	DepthRange  int64  `db:"depth_range" json:"-"`
}

type ChairSearchResponse struct {
	Count  int64   `json:"count"`
	Chairs []Chair `json:"chairs"`
}

type ChairListResponse struct {
	Chairs []Chair `json:"chairs"`
}

//Estate 物件
type Estate struct {
	ID              int64   `db:"id" json:"id"`
	Thumbnail       string  `db:"thumbnail" json:"thumbnail"`
	Name            string  `db:"name" json:"name"`
	Description     string  `db:"description" json:"description"`
	Latitude        float64 `db:"latitude" json:"latitude"`
	Longitude       float64 `db:"longitude" json:"longitude"`
	Address         string  `db:"address" json:"address"`
	Rent            int64   `db:"rent" json:"rent"`
	DoorHeight      int64   `db:"door_height" json:"doorHeight"`
	DoorWidth       int64   `db:"door_width" json:"doorWidth"`
	Features        string  `db:"features" json:"features"`
	Popularity      int64   `db:"popularity" json:"-"`
	Point           []byte  `db:"point" json:"-"`
	DoorHeightRange int64   `db:"door_height_range" json:"-"`
	DoorWidthRange  int64   `db:"door_width_range" json:"-"`
	RentRange       int64   `db:"rent_range" json:"-"`
}

//EstateSearchResponse estate/searchへのレスポンスの形式
type EstateSearchResponse struct {
	Count   int64    `json:"count"`
	Estates []Estate `json:"estates"`
}

type EstateListResponse struct {
	Estates []Estate `json:"estates"`
}

type Coordinate struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type Coordinates struct {
	Coordinates []Coordinate `json:"coordinates"`
}

type Range struct {
	ID  int64 `json:"id"`
	Min int64 `json:"min"`
	Max int64 `json:"max"`
}

type RangeCondition struct {
	Prefix string   `json:"prefix"`
	Suffix string   `json:"suffix"`
	Ranges []*Range `json:"ranges"`
}

type ListCondition struct {
	List []string `json:"list"`
}

type EstateSearchCondition struct {
	DoorWidth  RangeCondition `json:"doorWidth"`
	DoorHeight RangeCondition `json:"doorHeight"`
	Rent       RangeCondition `json:"rent"`
	Feature    ListCondition  `json:"feature"`
}

type ChairSearchCondition struct {
	Width   RangeCondition `json:"width"`
	Height  RangeCondition `json:"height"`
	Depth   RangeCondition `json:"depth"`
	Price   RangeCondition `json:"price"`
	Color   ListCondition  `json:"color"`
	Feature ListCondition  `json:"feature"`
	Kind    ListCondition  `json:"kind"`
}

type BoundingBox struct {
	// TopLeftCorner 緯度経度が共に最小値になるような点の情報を持っている
	TopLeftCorner Coordinate
	// BottomRightCorner 緯度経度が共に最大値になるような点の情報を持っている
	BottomRightCorner Coordinate
}

type MySQLConnectionEnv struct {
	Host     string
	Port     string
	User     string
	DBName   string
	Password string
}

type RecordMapper struct {
	Record []string

	offset int
	err    error
}

func (r *RecordMapper) next() (string, error) {
	if r.err != nil {
		return "", r.err
	}
	if r.offset >= len(r.Record) {
		r.err = fmt.Errorf("too many read")
		return "", r.err
	}
	s := r.Record[r.offset]
	r.offset++
	return s, nil
}

func (r *RecordMapper) NextInt() int {
	s, err := r.next()
	if err != nil {
		return 0
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		r.err = err
		return 0
	}
	return i
}

func (r *RecordMapper) NextFloat() float64 {
	s, err := r.next()
	if err != nil {
		return 0
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		r.err = err
		return 0
	}
	return f
}

func (r *RecordMapper) NextString() string {
	s, err := r.next()
	if err != nil {
		return ""
	}
	return s
}

func (r *RecordMapper) Err() error {
	return r.err
}

func NewMySQLConnectionEnv() *MySQLConnectionEnv {
	return &MySQLConnectionEnv{
		Host:     getEnv("MYSQL_HOST", "127.0.0.1"),
		Port:     getEnv("MYSQL_PORT", "3306"),
		User:     getEnv("MYSQL_USER", "isucon"),
		DBName:   getEnv("MYSQL_DBNAME", "isuumo"),
		Password: getEnv("MYSQL_PASS", "isucon"),
	}
}

func getEnv(key, defaultValue string) string {
	val := os.Getenv(key)
	if val != "" {
		return val
	}
	return defaultValue
}

//ConnectDB isuumoデータベースに接続する
func (mc *MySQLConnectionEnv) ConnectDB() (*sqlx.DB, error) {
	dsn := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?interpolateParams=true", mc.User, mc.Password, mc.Host, mc.Port, mc.DBName)
	return sqlx.Open(tracedDriver("mysql"), dsn)
}

func init() {
	jsonText, err := ioutil.ReadFile("../fixture/chair_condition.json")
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	json.Unmarshal(jsonText, &chairSearchCondition)
	chairSearchConditionJSON, _ = json.Marshal(chairSearchCondition)

	jsonText, err = ioutil.ReadFile("../fixture/estate_condition.json")
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	json.Unmarshal(jsonText, &estateSearchCondition)
	estateSearchConditionJSON, _ = json.Marshal(estateSearchCondition)
}

var botUA = []*regexp.Regexp{
	regexp.MustCompile(`^/$`),
	regexp.MustCompile(`(ISUCONbot-Image\/|Mediapartners-ISUCON|ISUCONCoffee|ISUCONFeedSeeker(Beta)?|crawler \(https:\/\/isucon\.invalid\/(support\/faq\/|help\/jp\/)|isubot|Isupider|Isupider(-image)?\+)`),
	/*regexp.MustCompile(`ISUCONbot-Image\/`),
	regexp.MustCompile(`Mediapartners-ISUCON`),
	regexp.MustCompile(`ISUCONCoffee`),
	regexp.MustCompile(`ISUCONFeedSeeker(Beta)?`),
	regexp.MustCompile(`crawler \(https:\/\/isucon\.invalid\/(support\/faq\/|help\/jp\/)`),
	regexp.MustCompile(`isubot`),
	regexp.MustCompile(`Isupider`),
	regexp.MustCompile(`Isupider(-image)?\+`),*/
	regexp.MustCompile(`(?i)(bot|crawler|spider)(?:[-_ .\/;@()]|$)`),
}

// Banbot middleware ban bot
func Banbot(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		ua := c.Request().UserAgent()
		for _, r := range botUA {
			if r.MatchString(ua) {
				return c.NoContent(http.StatusForbidden)
			}
		}
		return next(c)
	}
}

func WithTrace(next echo.HandlerFunc) echo.HandlerFunc {
	return func(ctx echo.Context) (err error) {
		handler := &ochttp.Handler{
			Handler: http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					ctx.SetRequest(r)
					ctx.SetResponse(echo.NewResponse(w, ctx.Echo()))
					_ = next(ctx)
				},
			),
		}
		handler.ServeHTTP(ctx.Response(), ctx.Request())
		return
	}
}

func main() {
	initProfiler()
	initTrace()

	chairCache.Flush()
	estateCache.Flush()

	// Echo instance
	e := echo.New()
	e.Debug = false
	// e.Logger.SetLevel(log.DEBUG)

	// Middleware
	// e.Use(middleware.Logger())
	//e.Use(WithTrace)
	//e.Use(middleware.Recover())
	e.Use(Banbot)

	// Initialize
	e.POST("/initialize", initialize)

	// Chair Handler
	e.GET("/api/chair/:id", getChairDetail)
	e.POST("/api/chair", postChair)
	e.GET("/api/chair/search", searchChairs)
	e.GET("/api/chair/low_priced", getLowPricedChair)
	e.GET("/api/chair/search/condition", getChairSearchCondition)
	e.POST("/api/chair/buy/:id", buyChair)

	// Estate Handler
	e.GET("/api/estate/:id", getEstateDetail)
	e.POST("/api/estate", postEstate)
	e.GET("/api/estate/search", searchEstates)
	e.GET("/api/estate/low_priced", getLowPricedEstate)
	e.POST("/api/estate/req_doc/:id", postEstateRequestDocument)
	e.POST("/api/estate/nazotte", searchEstateNazotte)
	e.GET("/api/estate/search/condition", getEstateSearchCondition)
	e.GET("/api/recommended_estate/:id", searchRecommendedEstateWithChair)

	mySQLConnectionData = NewMySQLConnectionEnv()

	var err error
	db, err = mySQLConnectionData.ConnectDB()
	if err != nil {
		e.Logger.Fatalf("DB connection failed : %v", err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	defer db.Close()

	// Start server
	serverPort := fmt.Sprintf(":%v", getEnv("SERVER_PORT", "1323"))
	e.Logger.Fatal(e.Start(serverPort))
}

func initialize(c echo.Context) error {
	_ = c.Request().Context()

	chairCache.Flush()
	estateCache.Flush()

	sqlDir := filepath.Join("..", "mysql", "db")
	paths := []string{
		filepath.Join(sqlDir, "0_Schema.sql"),
		filepath.Join(sqlDir, "1_DummyEstateData.sql"),
		filepath.Join(sqlDir, "2_DummyChairData.sql"),
	}

	for _, p := range paths {
		sqlFile, _ := filepath.Abs(p)
		cmdStr := fmt.Sprintf("mysql -h %v -u %v -p%v -P %v %v < %v",
			mySQLConnectionData.Host,
			mySQLConnectionData.User,
			mySQLConnectionData.Password,
			mySQLConnectionData.Port,
			mySQLConnectionData.DBName,
			sqlFile,
		)
		if err := exec.Command("bash", "-c", cmdStr).Run(); err != nil {
			c.Logger().Errorf("Initialize script error : %v", err)
			return c.NoContent(http.StatusInternalServerError)
		}
	}

	estateObjCache.Flush()
	estates := []Estate{}
	_ = db.Select(&estates, `SELECT * FROM estate`)
	for _, e := range estates {
		estateObjCache.Set(e.ID, e)
	}

	chairObjCache.Flush()
	chairs := []Chair{}
	_ = db.Select(&chairs, `SELECT * FROM chair`)
	for _, e := range chairs {
		chairObjCache.Set(e.ID, e)
	}

	return c.JSON(http.StatusOK, InitializeResponse{
		Language: "go",
	})
}

func getChairDetail(c echo.Context) error {
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.Echo().Logger.Errorf("Request parameter \"id\" parse error : %v", err)
		return c.NoContent(http.StatusBadRequest)
	}

	if e, ok := chairObjCache.Get(int64(id)); ok {
		if e.Stock <= 0 {
			return c.NoContent(http.StatusNotFound)
		}
		return c.JSON(http.StatusOK, e)
	}

	chair := Chair{}
	query := `SELECT * FROM chair WHERE id = ?`
	err = db.Get(&chair, query, id)
	if err != nil {
		if err == sql.ErrNoRows {
			c.Echo().Logger.Infof("requested id's chair not found : %v", id)
			return c.NoContent(http.StatusNotFound)
		}
		c.Echo().Logger.Errorf("Failed to get the chair from id : %v", err)
		return c.NoContent(http.StatusInternalServerError)
	} else if chair.Stock <= 0 {
		c.Echo().Logger.Infof("requested id's chair is sold out : %v", id)
		return c.NoContent(http.StatusNotFound)
	}

	return c.JSON(http.StatusOK, chair)
}

func postChair(c echo.Context) error {
	header, err := c.FormFile("chairs")
	if err != nil {
		c.Logger().Errorf("failed to get form file: %v", err)
		return c.NoContent(http.StatusBadRequest)
	}
	f, err := header.Open()
	if err != nil {
		c.Logger().Errorf("failed to open form file: %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}
	defer f.Close()
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		c.Logger().Errorf("failed to read csv: %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}

	//time.Sleep(1500 * time.Millisecond)

	maxID := 0
	_ = db.Get(&maxID, `SELECT max(id) FROM chair`)
	log.Printf("max_chair: %d", maxID)

	tx, err := db.Begin()
	if err != nil {
		c.Logger().Errorf("failed to begin tx: %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}
	defer tx.Rollback()

	recordsChunk := splitRecords(records, 1000)
	for _, records := range recordsChunk {
		query := "INSERT INTO chair(id, name, description, thumbnail, price, height, width, depth, color, features, kind, popularity, stock) VALUES "
		values := make([]string, 0)
		params := make([]interface{}, 0)
		for _, row := range records {
			rm := RecordMapper{Record: row}
			id := rm.NextInt()
			name := rm.NextString()
			description := rm.NextString()
			thumbnail := rm.NextString()
			price := rm.NextInt()
			height := rm.NextInt()
			width := rm.NextInt()
			depth := rm.NextInt()
			color := rm.NextString()
			features := rm.NextString()
			kind := rm.NextString()
			popularity := rm.NextInt()
			stock := rm.NextInt()
			if err := rm.Err(); err != nil {
				c.Logger().Errorf("failed to read record: %v", err)
				return c.NoContent(http.StatusBadRequest)
			}
			values = append(values, "(?,?,?,?,?,?,?,?,?,?,?,?,?)")
			params = append(params, id, name, description, thumbnail, price, height, width, depth, color, features, kind, popularity, stock)
		}

		insertValues := strings.Join(values, ",")
		_, err := tx.Exec(query+insertValues, params...)
		if err != nil {
			c.Logger().Errorf("failed to insert chair: %v", err)
			return c.NoContent(http.StatusInternalServerError)
		}
	}

	if err := tx.Commit(); err != nil {
		c.Logger().Errorf("failed to commit tx: %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}

	chairs := []Chair{}
	_ = db.Select(&chairs, `SELECT * FROM chair WHERE id > ?`, maxID)
	// log.Printf("cache set %d", len(estates))
	for _, e := range chairs {
		chairObjCache.Set(e.ID, e)
	}
	chairCache.Flush()

	return c.NoContent(http.StatusCreated)
}

func applyChairs(ids []int64) []Chair {
	res, _ := chairObjCache.GetMulti(ids)
	return res
}

func searchChairs(c echo.Context) error {
	conditions := make([]string, 0)
	params := make([]interface{}, 0)

	qs := c.QueryString()
	if r, found := chairCache.Get(qs); found {
		return c.JSONBlob(http.StatusOK, r.([]byte))
	}

	if c.QueryParam("priceRangeId") != "" {
		chairPrice, err := getRange(chairSearchCondition.Price, c.QueryParam("priceRangeId"))
		if err != nil {
			c.Echo().Logger.Infof("priceRangeID invalid, %v : %v", c.QueryParam("priceRangeId"), err)
			return c.NoContent(http.StatusBadRequest)
		}
		conditions = append(conditions, "price_range = ?")
		params = append(params, chairPrice.ID)
	}

	if c.QueryParam("heightRangeId") != "" {
		chairHeight, err := getRange(chairSearchCondition.Height, c.QueryParam("heightRangeId"))
		if err != nil {
			c.Echo().Logger.Infof("heightRangeIf invalid, %v : %v", c.QueryParam("heightRangeId"), err)
			return c.NoContent(http.StatusBadRequest)
		}
		conditions = append(conditions, "height_range = ?")
		params = append(params, chairHeight.ID)
	}

	if c.QueryParam("widthRangeId") != "" {
		chairWidth, err := getRange(chairSearchCondition.Width, c.QueryParam("widthRangeId"))
		if err != nil {
			c.Echo().Logger.Infof("widthRangeID invalid, %v : %v", c.QueryParam("widthRangeId"), err)
			return c.NoContent(http.StatusBadRequest)
		}
		conditions = append(conditions, "width_range = ?")
		params = append(params, chairWidth.ID)
	}

	if c.QueryParam("depthRangeId") != "" {
		chairDepth, err := getRange(chairSearchCondition.Depth, c.QueryParam("depthRangeId"))
		if err != nil {
			c.Echo().Logger.Infof("depthRangeId invalid, %v : %v", c.QueryParam("depthRangeId"), err)
			return c.NoContent(http.StatusBadRequest)
		}
		conditions = append(conditions, "depth_range = ?")
		params = append(params, chairDepth.ID)
	}

	if c.QueryParam("kind") != "" {
		conditions = append(conditions, "kind = ?")
		params = append(params, c.QueryParam("kind"))
	}

	if c.QueryParam("color") != "" {
		conditions = append(conditions, "color = ?")
		params = append(params, c.QueryParam("color"))
	}

	if c.QueryParam("features") != "" {
		for _, f := range strings.Split(c.QueryParam("features"), ",") {
			conditions = append(conditions, "features LIKE CONCAT('%', ?, '%')")
			params = append(params, f)
		}
	}

	if len(conditions) == 0 {
		c.Echo().Logger.Infof("Search condition not found")
		return c.NoContent(http.StatusBadRequest)
	}

	page, err := strconv.Atoi(c.QueryParam("page"))
	if err != nil {
		c.Logger().Infof("Invalid format page parameter : %v", err)
		return c.NoContent(http.StatusBadRequest)
	}

	perPage, err := strconv.Atoi(c.QueryParam("perPage"))
	if err != nil {
		c.Logger().Infof("Invalid format perPage parameter : %v", err)
		return c.NoContent(http.StatusBadRequest)
	}

	searchQuery := "SELECT id FROM chair_stock WHERE "
	countQuery := "SELECT COUNT(*) FROM chair_stock WHERE "
	searchCondition := strings.Join(conditions, " AND ")
	limitOffset := " ORDER BY popularity DESC, id ASC LIMIT ? OFFSET ?"
	//log.Printf("searchCondition: %s", searchCondition)
	var res ChairSearchResponse
	ckj, _ := json.Marshal(params)
	countKey := searchCondition + string(ckj)
	r, found := chairCache.Get(countKey)
	if found {
		//log.Printf("Hit %s", countKey)
		res.Count = r.(int64)
	}
	var cntErr error
	var wg sync.WaitGroup
	if !found {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cntErr = db.Get(&res.Count, countQuery+searchCondition, params...)
			if cntErr == nil {
				chairCache.Set(countKey, res.Count)
			}
		}()
	}

	ids := []int64{}
	paramsQ := append(params, perPage, page*perPage)
	err = db.Select(&ids, searchQuery+searchCondition+limitOffset, paramsQ...)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.JSON(http.StatusOK, ChairSearchResponse{Count: 0, Chairs: []Chair{}})
		}
		c.Logger().Errorf("searchChairs DB execution error : %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}

	wg.Wait()
	if cntErr != nil {
		c.Logger().Errorf("searchChairs DB execution error : %v", cntErr)
		return c.NoContent(http.StatusInternalServerError)
	}

	res.Chairs = applyChairs(ids)
	b, _ := json.Marshal(res)
	chairCache.Set(qs, b)

	return c.JSONBlob(http.StatusOK, b)
}

func buyChair(c echo.Context) error {
	m := echo.Map{}
	if err := c.Bind(&m); err != nil {
		c.Echo().Logger.Infof("post buy chair failed : %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}

	_, ok := m["email"].(string)
	if !ok {
		c.Echo().Logger.Info("post buy chair failed : email not found in request body")
		return c.NoContent(http.StatusBadRequest)
	}

	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.Echo().Logger.Infof("post buy chair failed : %v", err)
		return c.NoContent(http.StatusBadRequest)
	}

	result, err := db.Exec("UPDATE chair SET stock = stock - 1 WHERE id = ? AND stock > 0", id)
	if err != nil {
		c.Echo().Logger.Errorf("chair stock update failed : %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		c.Echo().Logger.Errorf("chair stock update failed : %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}

	if rowsAffected == 0 {
		return c.NoContent(http.StatusNotFound)
	}

	if e, ok := chairObjCache.Get(int64(id)); ok {
		e.Stock = e.Stock - 1
		chairObjCache.Set(e.ID, e)
	}
	chairCache.Flush()

	return c.NoContent(http.StatusOK)
}

func getChairSearchCondition(c echo.Context) error {
	return c.JSONBlob(http.StatusOK, chairSearchConditionJSON)
}

func getLowPricedChair(c echo.Context) error {
	if r, found := chairCache.Get("getLowPricedChair"); found {
		return c.JSONBlob(http.StatusOK, r.([]byte))
	}

	r, e, _ := sfGroup.Do("getLowPricedChair", func() (interface{}, error) {
		var chairs []Chair
		query := `SELECT * FROM chair_stock ORDER BY price ASC, id ASC LIMIT ?`
		err := db.Select(&chairs, query, Limit)
		if err != nil {
			if err == sql.ErrNoRows {
				c.Logger().Error("getLowPricedChair not found")
				return []byte("[]"), nil
			}
			c.Logger().Errorf("getLowPricedChair DB execution error : %v", err)
			return nil, err
		}
		b, _ := json.Marshal(ChairListResponse{Chairs: chairs})
		chairCache.Set("getLowPricedChair", b)

		return b, nil
	})
	if e != nil {
		return c.NoContent(http.StatusInternalServerError)
	}
	return c.JSONBlob(http.StatusOK, r.([]byte))
}

func getEstateDetail(c echo.Context) error {
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.Echo().Logger.Infof("Request parameter \"id\" parse error : %v", err)
		return c.NoContent(http.StatusBadRequest)
	}

	if cacheEs, ok := estateObjCache.Get(int64(id)); ok {
		return c.JSON(http.StatusOK, cacheEs)
	}

	var estate Estate
	err = db.Get(&estate, "SELECT * FROM estate WHERE id = ?", id)
	if err != nil {
		if err == sql.ErrNoRows {
			c.Echo().Logger.Infof("getEstateDetail estate id %v not found", id)
			return c.NoContent(http.StatusNotFound)
		}
		c.Echo().Logger.Errorf("Database Execution error : %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.JSON(http.StatusOK, estate)
}

func getRange(cond RangeCondition, rangeID string) (*Range, error) {
	RangeIndex, err := strconv.Atoi(rangeID)
	if err != nil {
		return nil, err
	}

	if RangeIndex < 0 || len(cond.Ranges) <= RangeIndex {
		return nil, fmt.Errorf("Unexpected Range ID")
	}

	return cond.Ranges[RangeIndex], nil
}

func postEstate(c echo.Context) error {
	header, err := c.FormFile("estates")
	if err != nil {
		c.Logger().Errorf("failed to get form file: %v", err)
		return c.NoContent(http.StatusBadRequest)
	}
	f, err := header.Open()
	if err != nil {
		c.Logger().Errorf("failed to open form file: %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}
	defer f.Close()
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		c.Logger().Errorf("failed to read csv: %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}

	maxID := 0
	_ = db.Get(&maxID, `SELECT max(id) FROM estate`)
	log.Printf("max_estate: %d", maxID)
	tx, err := db.Begin()
	if err != nil {
		c.Logger().Errorf("failed to begin tx: %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}
	defer tx.Rollback()

	query := "INSERT INTO estate(id, name, description, thumbnail, address, latitude, longitude, rent, door_height, door_width, features, popularity) VALUES "
	values := make([]string, 0)
	params := make([]interface{}, 0)
	for _, row := range records {
		rm := RecordMapper{Record: row}
		id := rm.NextInt()
		name := rm.NextString()
		description := rm.NextString()
		thumbnail := rm.NextString()
		address := rm.NextString()
		latitude := rm.NextFloat()
		longitude := rm.NextFloat()
		rent := rm.NextInt()
		doorHeight := rm.NextInt()
		doorWidth := rm.NextInt()
		features := rm.NextString()
		popularity := rm.NextInt()
		if err := rm.Err(); err != nil {
			c.Logger().Errorf("failed to read record: %v", err)
			return c.NoContent(http.StatusBadRequest)
		}

		values = append(values, "(?,?,?,?,?,?,?,?,?,?,?,?)")
		params = append(params, id, name, description, thumbnail, address, latitude, longitude, rent, doorHeight, doorWidth, features, popularity)
	}
	insertValues := strings.Join(values, ",")
	_, err = tx.Exec(query+insertValues, params...)
	if err != nil {
		c.Logger().Errorf("failed to insert estate: %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}

	if err := tx.Commit(); err != nil {
		c.Logger().Errorf("failed to commit tx: %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}

	estates := []Estate{}
	_ = db.Select(&estates, `SELECT * FROM estate WHERE id > ?`, maxID)
	// log.Printf("cache set %d", len(estates))
	for _, e := range estates {
		estateObjCache.Set(e.ID, e)
	}
	estateCache.Flush()

	return c.NoContent(http.StatusCreated)
}

func appluEstates(ids []int64) []Estate {
	res, _ := estateObjCache.GetMulti(ids)
	return res
}

func searchEstates(c echo.Context) error {
	conditions := make([]string, 0)
	params := make([]interface{}, 0)

	qs := c.QueryString()
	if r, found := estateCache.Get(qs); found {
		return c.JSONBlob(http.StatusOK, r.([]byte))
	}

	if c.QueryParam("doorHeightRangeId") != "" {
		doorHeight, err := getRange(estateSearchCondition.DoorHeight, c.QueryParam("doorHeightRangeId"))
		if err != nil {
			c.Echo().Logger.Infof("doorHeightRangeID invalid, %v : %v", c.QueryParam("doorHeightRangeId"), err)
			return c.NoContent(http.StatusBadRequest)
		}
		conditions = append(conditions, "door_height_range = ?")
		params = append(params, doorHeight.ID)
	}

	if c.QueryParam("doorWidthRangeId") != "" {
		doorWidth, err := getRange(estateSearchCondition.DoorWidth, c.QueryParam("doorWidthRangeId"))
		if err != nil {
			c.Echo().Logger.Infof("doorWidthRangeID invalid, %v : %v", c.QueryParam("doorWidthRangeId"), err)
			return c.NoContent(http.StatusBadRequest)
		}
		conditions = append(conditions, "door_width_range = ?")
		params = append(params, doorWidth.ID)
	}

	if c.QueryParam("rentRangeId") != "" {
		estateRent, err := getRange(estateSearchCondition.Rent, c.QueryParam("rentRangeId"))
		if err != nil {
			c.Echo().Logger.Infof("rentRangeID invalid, %v : %v", c.QueryParam("rentRangeId"), err)
			return c.NoContent(http.StatusBadRequest)
		}
		conditions = append(conditions, "rent_range = ?")
		params = append(params, estateRent.ID)
	}

	if c.QueryParam("features") != "" {
		for _, f := range strings.Split(c.QueryParam("features"), ",") {
			conditions = append(conditions, "features like concat('%', ?, '%')")
			params = append(params, f)
		}
	}

	if len(conditions) == 0 {
		c.Echo().Logger.Infof("searchEstates search condition not found")
		return c.NoContent(http.StatusBadRequest)
	}

	page, err := strconv.Atoi(c.QueryParam("page"))
	if err != nil {
		c.Logger().Infof("Invalid format page parameter : %v", err)
		return c.NoContent(http.StatusBadRequest)
	}

	perPage, err := strconv.Atoi(c.QueryParam("perPage"))
	if err != nil {
		c.Logger().Infof("Invalid format perPage parameter : %v", err)
		return c.NoContent(http.StatusBadRequest)
	}

	searchQuery := "SELECT id FROM estate WHERE "
	countQuery := "SELECT COUNT(*) FROM estate WHERE "
	searchCondition := strings.Join(conditions, " AND ")
	limitOffset := " ORDER BY popularity DESC, id ASC LIMIT ? OFFSET ?"
	//fmt.Printf("%s\n", searchCondition)
	var res EstateSearchResponse
	ckj, _ := json.Marshal(params)
	countKey := searchCondition + string(ckj)
	r, found := estateCache.Get(countKey)
	if found {
		//log.Printf("Hit %s", countKey)
		res.Count = r.(int64)
	}

	var cntErr error
	var wg sync.WaitGroup
	if !found {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cntErr = db.Get(&res.Count, countQuery+searchCondition, params...)
			if cntErr == nil {
				estateCache.Set(countKey, res.Count)
			}
		}()
	}

	//estates := []Estate{}
	ids := []int64{}
	paramsQ := append(params, perPage, page*perPage)
	err = db.Select(&ids, searchQuery+searchCondition+limitOffset, paramsQ...)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.JSON(http.StatusOK, EstateSearchResponse{Count: 0, Estates: []Estate{}})
		}
		c.Logger().Errorf("searchEstates DB execution error : %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}

	wg.Wait()
	if cntErr != nil {
		c.Logger().Errorf("searchEstates DB execution error : %v", cntErr)
		return c.NoContent(http.StatusInternalServerError)
	}

	res.Estates = appluEstates(ids)
	b, _ := json.Marshal(res)
	estateCache.Set(qs, b)

	return c.JSONBlob(http.StatusOK, b)
}

func getLowPricedEstate(c echo.Context) error {
	if r, found := estateCache.Get("getLowPricedEstate"); found {
		return c.JSONBlob(http.StatusOK, r.([]byte))
	}
	r, e, _ := sfGroup.Do("getLowPricedEstate", func() (interface{}, error) {

		estates := make([]Estate, 0, Limit)
		query := `SELECT * FROM estate ORDER BY rent ASC, id ASC LIMIT ?`
		err := db.Select(&estates, query, Limit)
		if err != nil {
			if err != sql.ErrNoRows {
				c.Logger().Error("getLowPricedEstate not found")
				return []byte("[]"), nil
			}
			c.Logger().Errorf("getLowPricedEstate DB execution error : %v", err)
			return nil, err
		}

		b, _ := json.Marshal(EstateListResponse{Estates: estates})
		estateCache.Set("getLowPricedEstate", b)

		return b, nil
	})
	if e != nil {
		return c.NoContent(http.StatusInternalServerError)
	}
	return c.JSONBlob(http.StatusOK, r.([]byte))
}

func searchRecommendedEstateWithChair(c echo.Context) error {
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.Logger().Infof("Invalid format searchRecommendedEstateWithChair id : %v", err)
		return c.NoContent(http.StatusBadRequest)
	}

	chair, ok := chairObjCache.Get(int64(id))
	if !ok {
		return c.NoContent(http.StatusBadRequest)
	}

	if r, found := estateCache.Get(fmt.Sprintf("recom%d", id)); found {
		//log.Printf("recom hit")
		return c.JSONBlob(http.StatusOK, r.([]byte))
	}
	/*chair := Chair{}
	query := `SELECT * FROM chair WHERE id = ?`
	err = db.Get(&chair, query, id)
	if err != nil {
		if err == sql.ErrNoRows {
			c.Logger().Infof("Requested chair id \"%v\" not found", id)
			return c.NoContent(http.StatusBadRequest)
		}
		c.Logger().Errorf("Database execution error : %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}*/

	//var estates []Estate
	w := chair.Width
	h := chair.Height
	d := chair.Depth
	len := []int64{w, h, d}
	sort.Slice(len, func(i, j int) bool {
		return len[i] < len[j]
	})
	minLen := len[0]
	midLen := len[1]

	query := `SELECT id FROM estate FORCE INDEX(idx_pop) WHERE (door_width >= ? AND door_height >= ?) OR (door_width >= ? AND door_height >= ?) ORDER BY popularity DESC, id ASC LIMIT ?`
	ids := []int64{}
	err = db.Select(&ids, query, minLen, midLen, midLen, minLen, Limit)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.JSON(http.StatusOK, EstateListResponse{[]Estate{}})
		}
		c.Logger().Errorf("Database execution error : %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}

	b, _ := json.Marshal(EstateListResponse{Estates: appluEstates(ids)})
	estateCache.Set(fmt.Sprintf("recom%d", id), b)

	return c.JSONBlob(http.StatusOK, b)
}

func searchEstateNazotte(c echo.Context) error {
	coordinates := Coordinates{}
	err := c.Bind(&coordinates)
	if err != nil {
		c.Echo().Logger.Infof("post search estate nazotte failed : %v", err)
		return c.NoContent(http.StatusBadRequest)
	}

	if len(coordinates.Coordinates) == 0 {
		return c.NoContent(http.StatusBadRequest)
	}

	// log.Printf("%s", coordinates.coordinatesToText())
	//estatesInPolygon := []Estate{}
	query := fmt.Sprintf("SELECT id FROM estate WHERE ST_Contains(ST_PolygonFromText(%s), point)", coordinates.coordinatesToText())
	orderBy := " ORDER BY popularity DESC, id ASC LIMIT 50"
	ids := []int64{}
	err = db.Select(&ids, query+orderBy)
	var re EstateSearchResponse
	re.Estates = appluEstates(ids)
	re.Count = int64(len(re.Estates))

	return c.JSON(http.StatusOK, re)
}

func postEstateRequestDocument(c echo.Context) error {
	m := echo.Map{}
	if err := c.Bind(&m); err != nil {
		c.Echo().Logger.Infof("post request document failed : %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}

	_, ok := m["email"].(string)
	if !ok {
		c.Echo().Logger.Info("post request document failed : email not found in request body")
		return c.NoContent(http.StatusBadRequest)
	}

	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.Echo().Logger.Infof("post request document failed : %v", err)
		return c.NoContent(http.StatusBadRequest)
	}

	if _, ok := estateObjCache.Get(int64(id)); ok {
		return c.NoContent(http.StatusOK)
	}

	estate := Estate{}
	query := `SELECT * FROM estate WHERE id = ?`
	err = db.Get(&estate, query, id)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.NoContent(http.StatusNotFound)
		}
		c.Logger().Errorf("postEstateRequestDocument DB execution error : %v", err)
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.NoContent(http.StatusOK)
}

func getEstateSearchCondition(c echo.Context) error {
	return c.JSONBlob(http.StatusOK, estateSearchConditionJSON)
}

func (cs Coordinates) getBoundingBox() BoundingBox {
	coordinates := cs.Coordinates
	boundingBox := BoundingBox{
		TopLeftCorner: Coordinate{
			Latitude: coordinates[0].Latitude, Longitude: coordinates[0].Longitude,
		},
		BottomRightCorner: Coordinate{
			Latitude: coordinates[0].Latitude, Longitude: coordinates[0].Longitude,
		},
	}
	for _, coordinate := range coordinates {
		if boundingBox.TopLeftCorner.Latitude > coordinate.Latitude {
			boundingBox.TopLeftCorner.Latitude = coordinate.Latitude
		}
		if boundingBox.TopLeftCorner.Longitude > coordinate.Longitude {
			boundingBox.TopLeftCorner.Longitude = coordinate.Longitude
		}

		if boundingBox.BottomRightCorner.Latitude < coordinate.Latitude {
			boundingBox.BottomRightCorner.Latitude = coordinate.Latitude
		}
		if boundingBox.BottomRightCorner.Longitude < coordinate.Longitude {
			boundingBox.BottomRightCorner.Longitude = coordinate.Longitude
		}
	}
	return boundingBox
}

func (cs Coordinates) coordinatesToText() string {
	points := make([]string, 0, len(cs.Coordinates))
	for _, c := range cs.Coordinates {
		points = append(points, fmt.Sprintf("%f %f", c.Latitude, c.Longitude))
	}
	return fmt.Sprintf("'POLYGON((%s))'", strings.Join(points, ","))
}

func splitRecords(records [][]string, n int) [][][]string {
	ret := make([][][]string, len(records)/n)

	for i := 0; i < len(records); i += n {
		end := i + n
		if len(records) < end {
			end = len(records)
		}
		ret = append(ret, records[i:end])
	}
	return ret
}
