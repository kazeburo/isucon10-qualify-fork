package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/isucon/isucon10-qualify/isuumo/types"
	"github.com/mailru/easyjson/jwriter"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gofiber/fiber/v2"
	"github.com/jmoiron/sqlx"
	"github.com/niubaoshu/gotiny"
	"golang.org/x/sync/singleflight"
)

const Limit = 20
const NazotteLimit = 50

var db *sqlx.DB
var mySQLConnectionData *MySQLConnectionEnv
var chairSearchCondition types.ChairSearchCondition
var estateSearchCondition types.EstateSearchCondition
var chairSearchConditionJSON []byte
var estateSearchConditionJSON []byte

var chairCache = NewSC()
var chairObjCache = NewCC()
var estateCache = NewSC()
var estateObjCache = NewIC()
var sfGroup singleflight.Group

var estatePool = sync.Pool{
	New: func() interface{} {
		s := make([]types.Estate, 0, 50)
		return &s
	},
}

var chairPool = sync.Pool{
	New: func() interface{} {
		s := make([]types.Chair, 0, 50)
		return &s
	},
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

func UnsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
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
	chairSearchConditionJSON, _ = chairSearchCondition.MarshalJSON()

	jsonText, err = ioutil.ReadFile("../fixture/estate_condition.json")
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	json.Unmarshal(jsonText, &estateSearchCondition)
	estateSearchConditionJSON, _ = estateSearchCondition.MarshalJSON()
}

//var botUA = []*regexp.Regexp{
//regexp.MustCompile(`^/$`),
//regexp.MustCompile(`(ISUCONbot-Image\/|Mediapartners-ISUCON|ISUCONCoffee|ISUCONFeedSeeker(Beta)?|crawler \(https:\/\/isucon\.invalid\/(support\/faq\/|help\/jp\/)|isubot|Isupider|Isupider(-image)?\+)`),
/*regexp.MustCompile(`ISUCONbot-Image\/`),
regexp.MustCompile(`Mediapartners-ISUCON`),
regexp.MustCompile(`ISUCONCoffee`),
regexp.MustCompile(`ISUCONFeedSeeker(Beta)?`),
regexp.MustCompile(`crawler \(https:\/\/isucon\.invalid\/(support\/faq\/|help\/jp\/)`),
regexp.MustCompile(`isubot`),
regexp.MustCompile(`Isupider`),
regexp.MustCompile(`Isupider(-image)?\+`),*/
//	regexp.MustCompile(`(?i)(bot|crawler|spider)(?:[-_ .\/;@()]|$)`),
//}

// Banbot middleware ban bot
var botUAb = [][]byte{
	[]byte("ISUCONbot-Image/"),
	[]byte("Mediapartners-ISUCON"),
	[]byte("ISUCONCoffee"),
	[]byte("ISUCONFeedSeeker"),
	[]byte("crawler (https://isucon.invalid/support/faq/"),
	[]byte("crawler (https://isucon.invalid/help/jp/"),
	[]byte("isubot"),
	[]byte("Isupider"),
}
var spiderSpecialKey = []byte{
	'-',
	'_',
	' ',
	'.',
	'/',
	';',
	'@',
	'(',
	')',
}

func uaLikeBot(ua []byte) bool {
	for _, b := range botUAb {
		if bytes.Contains(ua, b) {
			return true
		}
	}
	return false
}

func uaLikeSpider(d []byte, s []byte) bool {
	p1 := 0
	dlen := len(d)
	slen := len(s)
	found := false
PARENT:
	for {
		if dlen == p1 {
			break
		}
		p2 := bytes.Index(d[p1:], s)
		if p2 < 0 {
			break
		}
		p3 := p1 + p2 + slen
		if p3 == dlen {
			found = true
			break
		}
		for _, ssk := range spiderSpecialKey {
			if d[p3] == ssk {
				found = true
				break PARENT
			}
		}
		p1 = p3 + 1
	}
	return found
}

func Banbot(c *fiber.Ctx) error {
	ua := c.Request().Header.UserAgent()
	uaLower := bytes.ToLower(ua)
	if bytes.Equal(ua, []byte("/")) {
		return c.SendStatus(fiber.StatusForbidden)
	}
	if bytes.Contains(ua, []byte("isu")) && uaLikeBot(ua) {
		return c.SendStatus(fiber.StatusForbidden)
	}
	if uaLikeSpider(uaLower, []byte("bot")) ||
		uaLikeSpider(uaLower, []byte("crawler")) ||
		uaLikeSpider(uaLower, []byte("spider")) {
		return c.SendStatus(fiber.StatusForbidden)
	}
	return c.Next()
}

func main() {
	initProfiler()
	//initTrace()

	chairCache.Flush()
	estateCache.Flush()

	// Fiber instance
	e := fiber.New(fiber.Config{WriteBufferSize: 16 * 1024})

	// Middleware
	e.Use(Banbot)

	// Initialize
	e.Post("/initialize", initialize)

	// Chair Handler
	e.Post("/api/chair", postChair)
	e.Get("/api/chair/search", searchChairs)
	e.Get("/api/chair/low_priced", getLowPricedChair)
	e.Get("/api/chair/search/condition", getChairSearchCondition)
	e.Post("/api/chair/buy/:id", buyChair)
	e.Get("/api/chair/:id", getChairDetail)

	// Estate Handler
	e.Post("/api/estate", postEstate)
	e.Get("/api/estate/search", searchEstates)
	e.Get("/api/estate/low_priced", getLowPricedEstate)
	e.Post("/api/estate/req_doc/:id", postEstateRequestDocument)
	e.Post("/api/estate/nazotte", searchEstateNazotte)
	e.Get("/api/estate/search/condition", getEstateSearchCondition)
	e.Get("/api/recommended_estate/:id", searchRecommendedEstateWithChair)
	e.Get("/api/estate/:id", getEstateDetail)

	mySQLConnectionData = NewMySQLConnectionEnv()

	var err error
	db, err = mySQLConnectionData.ConnectDB()
	if err != nil {
		log.Fatal("DB connection failed : %v", err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	defer db.Close()

	estateObjCache.Flush()
	estates := []types.Estate{}
	_ = db.Select(&estates, `SELECT * FROM estate`)
	for _, e := range estates {
		estateObjCache.Set(e.ID, e)
	}

	chairObjCache.Flush()
	chairs := []types.Chair{}
	_ = db.Select(&chairs, `SELECT * FROM chair`)
	for _, e := range chairs {
		chairObjCache.Set(e.ID, e)
	}

	// Start server
	serverPort := fmt.Sprintf(":%v", getEnv("SERVER_PORT", "1323"))
	log.Fatal(e.Listen(serverPort))
}

func initialize(c *fiber.Ctx) error {

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
			log.Printf("Initialize script error : %v", err)
			return c.SendStatus(fiber.StatusInternalServerError)
		}
	}

	estateObjCache.Flush()
	estates := []types.Estate{}
	_ = db.Select(&estates, `SELECT * FROM estate`)
	for _, e := range estates {
		estateObjCache.Set(e.ID, e)
	}

	chairObjCache.Flush()
	chairs := []types.Chair{}
	_ = db.Select(&chairs, `SELECT * FROM chair`)
	for _, e := range chairs {
		chairObjCache.Set(e.ID, e)
	}

	return json.NewEncoder(c.Type("application/json").Status(fiber.StatusOK)).Encode(types.InitializeResponse{
		Language: "go",
	})
}

var bytesPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 16*1024)
	},
}

func getChairDetail(c *fiber.Ctx) error {
	id, err := strconv.Atoi(c.Params("id"))
	if err != nil {
		log.Printf("2 Request parameter \"id\" parse error : %v", err)
		return c.SendStatus(fiber.StatusBadRequest)
	}

	if e, ok := chairObjCache.Get(int64(id)); ok {
		if e.Stock <= 0 {
			return c.SendStatus(fiber.StatusNotFound)
		}
		s := bytesPool.Get().([]byte)
		defer bytesPool.Put(s)
		w := jwriter.Writer{}
		e.MarshalEasyJSON(&w)
		return c.Type("application/json").Status(fiber.StatusOK).Send(w.Buffer.BuildBytes(s))
	}

	chair := types.Chair{}

	query := `SELECT * FROM chair WHERE id = ?`
	err = db.Get(&chair, query, id)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Printf("requested id's chair not found : %v", id)
			return c.SendStatus(fiber.StatusNotFound)
		}
		log.Printf("Failed to get the chair from id : %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
	} else if chair.Stock <= 0 {
		log.Printf("requested id's chair is sold out : %v", id)
		return c.SendStatus(fiber.StatusNotFound)
	}

	s := bytesPool.Get().([]byte)
	defer bytesPool.Put(s)
	w := jwriter.Writer{}
	chair.MarshalEasyJSON(&w)
	return c.Type("application/json").Status(fiber.StatusOK).Send(w.Buffer.BuildBytes(s))
}

func postChair(c *fiber.Ctx) error {
	header, err := c.FormFile("chairs")
	if err != nil {
		log.Printf("failed to get form file: %v", err)
		return c.SendStatus(fiber.StatusBadRequest)
	}
	f, err := header.Open()
	if err != nil {
		log.Printf("failed to open form file: %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
	}
	defer f.Close()
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		log.Printf("failed to read csv: %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
	}

	//time.Sleep(1500 * time.Millisecond)

	maxID := 0
	_ = db.Get(&maxID, `SELECT max(id) FROM chair`)
	// log.Printf("max_chair: %d", maxID)

	tx, err := db.Begin()
	if err != nil {
		log.Printf("failed to begin tx: %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
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
				log.Printf("failed to read record: %v", err)
				return c.SendStatus(fiber.StatusBadRequest)
			}
			values = append(values, "(?,?,?,?,?,?,?,?,?,?,?,?,?)")
			params = append(params, id, name, description, thumbnail, price, height, width, depth, color, features, kind, popularity, stock)
		}

		insertValues := strings.Join(values, ",")
		_, err := tx.Exec(query+insertValues, params...)
		if err != nil {
			log.Printf("failed to insert chair: %v", err)
			return c.SendStatus(fiber.StatusInternalServerError)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("failed to commit tx: %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
	}

	chairs := []types.Chair{}
	_ = db.Select(&chairs, `SELECT * FROM chair WHERE id > ?`, maxID)
	// log.Printf("cache set %d", len(estates))
	for _, e := range chairs {
		chairObjCache.Set(e.ID, e)
	}
	b := makeLowPricedChair()
	chairCache.FlushWithNew("getLowPricedChair", b)

	return c.SendStatus(fiber.StatusCreated)
}

func applyChairs(ids []int64, arr *[]types.Chair) {
	chairObjCache.GetMulti(ids, arr)
}

func applyEstates(ids []int64, arr *[]types.Estate) {
	estateObjCache.GetMulti(ids, arr)
}

func JSONBlob(c *fiber.Ctx, status int, body []byte) error {
	return c.Status(status).Type("application/json").Send(body)
}

func searchChairs(c *fiber.Ctx) error {
	conditions := make([]string, 0, 8)
	params := make([]interface{}, 0, 8)

	qs := UnsafeString(c.Request().URI().QueryString())
	if r, found := chairCache.Get(qs); found {
		return JSONBlob(c, fiber.StatusOK, r.([]byte))
	}

	if c.Query("priceRangeId") != "" {
		chairPrice, err := getRange(chairSearchCondition.Price, c.Query("priceRangeId"))
		if err != nil {
			log.Printf("priceRangeID invalid, %v : %v", c.Query("priceRangeId"), err)
			return c.SendStatus(fiber.StatusBadRequest)
		}
		conditions = append(conditions, "price_range = ?")
		params = append(params, chairPrice.ID)
	}

	if c.Query("heightRangeId") != "" {
		chairHeight, err := getRange(chairSearchCondition.Height, c.Query("heightRangeId"))
		if err != nil {
			log.Printf("heightRangeIf invalid, %v : %v", c.Query("heightRangeId"), err)
			return c.SendStatus(fiber.StatusBadRequest)
		}
		conditions = append(conditions, "height_range = ?")
		params = append(params, chairHeight.ID)
	}

	if c.Query("widthRangeId") != "" {
		chairWidth, err := getRange(chairSearchCondition.Width, c.Query("widthRangeId"))
		if err != nil {
			log.Printf("widthRangeID invalid, %v : %v", c.Query("widthRangeId"), err)
			return c.SendStatus(fiber.StatusBadRequest)
		}
		conditions = append(conditions, "width_range = ?")
		params = append(params, chairWidth.ID)
	}

	if c.Query("depthRangeId") != "" {
		chairDepth, err := getRange(chairSearchCondition.Depth, c.Query("depthRangeId"))
		if err != nil {
			log.Printf("depthRangeId invalid, %v : %v", c.Query("depthRangeId"), err)
			return c.SendStatus(fiber.StatusBadRequest)
		}
		conditions = append(conditions, "depth_range = ?")
		params = append(params, chairDepth.ID)
	}

	if c.Query("kind") != "" {
		conditions = append(conditions, "kind = ?")
		params = append(params, c.Query("kind"))
	}

	if c.Query("color") != "" {
		conditions = append(conditions, "color = ?")
		params = append(params, c.Query("color"))
	}

	if c.Query("features") != "" {
		for _, f := range strings.Split(c.Query("features"), ",") {
			conditions = append(conditions, "features LIKE CONCAT('%', ?, '%')")
			params = append(params, f)
		}
	}

	if len(conditions) == 0 {
		log.Printf("Search condition not found")
		return c.SendStatus(fiber.StatusBadRequest)
	}

	page, err := strconv.Atoi(c.Query("page"))
	if err != nil {
		log.Printf("Invalid format page parameter : %v", err)
		return c.SendStatus(fiber.StatusBadRequest)
	}

	perPage, err := strconv.Atoi(c.Query("perPage"))
	if err != nil {
		log.Printf("Invalid format perPage parameter : %v", err)
		return c.SendStatus(fiber.StatusBadRequest)
	}

	searchQuery := "SELECT id FROM chair_stock WHERE "
	countQuery := "SELECT COUNT(*) FROM chair_stock WHERE "
	searchCondition := strings.Join(conditions, " AND ")
	limitOffset := " ORDER BY popularity DESC, id ASC LIMIT ? OFFSET ?"
	//log.Printf("searchCondition: %s", searchCondition)
	var res types.ChairSearchResponse
	countKey := searchCondition + UnsafeString(gotiny.Marshal(&params))
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

	ids := make([]int64, 0, perPage)
	paramsQ := append(params, perPage, page*perPage)
	err = db.Select(&ids, searchQuery+searchCondition+limitOffset, paramsQ...)
	if err != nil {
		if err == sql.ErrNoRows {
			return json.NewEncoder(c.Type("application/json").Status(fiber.StatusOK)).Encode(types.ChairSearchResponse{Count: 0, Chairs: &[]types.Chair{}})
		}
		log.Printf("searchChairs DB execution error : %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
	}

	wg.Wait()
	if cntErr != nil {
		log.Printf("searchChairs DB execution error : %v", cntErr)
		return c.SendStatus(fiber.StatusInternalServerError)
	}

	arr := chairPool.Get().(*[]types.Chair)
	applyChairs(ids, arr)
	defer chairPool.Put(arr)
	res.Chairs = arr
	b, _ := res.MarshalJSON()
	chairCache.Set(qs, b)

	return JSONBlob(c, fiber.StatusOK, b)
}

func buyChair(c *fiber.Ctx) error {
	m := map[string]interface{}{}
	if err := c.BodyParser(&m); err != nil {
		log.Printf("post buy chair failed : %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
	}

	_, ok := m["email"].(string)
	if !ok {
		log.Printf("post buy chair failed : email not found in request body")
		return c.SendStatus(fiber.StatusBadRequest)
	}

	id, err := strconv.Atoi(c.Params("id"))
	if err != nil {
		log.Printf("post buy chair failed : %v", err)
		return c.SendStatus(fiber.StatusBadRequest)
	}

	result, err := db.Exec("UPDATE chair SET stock = stock - 1 WHERE id = ? AND stock > 0", id)
	if err != nil {
		log.Printf("chair stock update failed : %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Printf("chair stock update failed : %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
	}

	if rowsAffected == 0 {
		return c.SendStatus(fiber.StatusNotFound)
	}

	if e, ok := chairObjCache.Get(int64(id)); ok {
		e.Stock = e.Stock - 1
		chairObjCache.Set(e.ID, e)
	}
	//b := makeLowPricedChair()
	//chairCache.FlushWithNew("getLowPricedChair", b)
	chairCache.Flush()

	return c.SendStatus(fiber.StatusOK)
}

func getChairSearchCondition(c *fiber.Ctx) error {
	return JSONBlob(c, fiber.StatusOK, chairSearchConditionJSON)
}

func makeLowPricedChair() []byte {
	ids := make([]int64, 0, 50)
	query := `SELECT id FROM chair_stock ORDER BY price ASC, id ASC LIMIT ?`
	err := db.Select(&ids, query, Limit)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Printf("getLowPricedChair not found")
			return []byte("[]")
		}
		log.Printf("getLowPricedChair DB execution error : %v", err)
		return nil
	}
	arr := chairPool.Get().(*[]types.Chair)
	applyChairs(ids, arr)
	defer chairPool.Put(arr)
	b, _ := types.ChairListResponse{Chairs: arr}.MarshalJSON()
	return b
}

func getLowPricedChair(c *fiber.Ctx) error {
	if r, found := chairCache.Get("getLowPricedChair"); found {
		return JSONBlob(c, fiber.StatusOK, r.([]byte))
	}
	r, e, _ := sfGroup.Do("getLowPricedChair", func() (interface{}, error) {
		b := makeLowPricedChair()
		chairCache.Set("getLowPricedChair", b)
		return b, nil
	})
	if e != nil {
		return c.SendStatus(fiber.StatusInternalServerError)
	}
	return JSONBlob(c, fiber.StatusOK, r.([]byte))
}

func getEstateDetail(c *fiber.Ctx) error {
	id, err := strconv.Atoi(c.Params("id"))
	if err != nil {
		log.Printf("1 Request parameter \"id\" parse error : %v", err)
		return c.SendStatus(fiber.StatusBadRequest)
	}

	if cacheEs, ok := estateObjCache.Get(int64(id)); ok {
		s := bytesPool.Get().([]byte)
		defer bytesPool.Put(s)
		w := jwriter.Writer{}
		cacheEs.MarshalEasyJSON(&w)
		return c.Type("application/json").Status(fiber.StatusOK).Send(w.Buffer.BuildBytes(s))
	}

	var estate types.Estate
	err = db.Get(&estate, "SELECT * FROM estate WHERE id = ?", id)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Printf("getEstateDetail estate id %v not found", id)
			return c.SendStatus(fiber.StatusNotFound)
		}
		log.Printf("Database Execution error : %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
	}
	s := bytesPool.Get().([]byte)
	defer bytesPool.Put(s)
	w := jwriter.Writer{}
	estate.MarshalEasyJSON(&w)
	return c.Type("application/json").Status(fiber.StatusOK).Send(w.Buffer.BuildBytes(s))
}

func getRange(cond types.RangeCondition, rangeID string) (*types.Range, error) {
	RangeIndex, err := strconv.Atoi(rangeID)
	if err != nil {
		return nil, err
	}

	if RangeIndex < 0 || len(cond.Ranges) <= RangeIndex {
		return nil, fmt.Errorf("Unexpected Range ID")
	}

	return cond.Ranges[RangeIndex], nil
}

func postEstate(c *fiber.Ctx) error {
	header, err := c.FormFile("estates")
	if err != nil {
		log.Printf("failed to get form file: %v", err)
		return c.SendStatus(fiber.StatusBadRequest)
	}
	f, err := header.Open()
	if err != nil {
		log.Printf("failed to open form file: %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
	}
	defer f.Close()
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		log.Printf("failed to read csv: %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
	}

	maxID := 0
	_ = db.Get(&maxID, `SELECT max(id) FROM estate`)
	log.Printf("max_estate: %d", maxID)
	tx, err := db.Begin()
	if err != nil {
		log.Printf("failed to begin tx: %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
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
			log.Printf("failed to read record: %v", err)
			return c.SendStatus(fiber.StatusBadRequest)
		}

		values = append(values, "(?,?,?,?,?,?,?,?,?,?,?,?)")
		params = append(params, id, name, description, thumbnail, address, latitude, longitude, rent, doorHeight, doorWidth, features, popularity)
	}
	insertValues := strings.Join(values, ",")
	_, err = tx.Exec(query+insertValues, params...)
	if err != nil {
		log.Printf("failed to insert estate: %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
	}

	if err := tx.Commit(); err != nil {
		log.Printf("failed to commit tx: %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
	}

	estates := []types.Estate{}
	_ = db.Select(&estates, `SELECT * FROM estate WHERE id > ?`, maxID)
	// log.Printf("cache set %d", len(estates))
	for _, e := range estates {
		estateObjCache.Set(e.ID, e)
	}
	estateCache.Flush()

	return c.SendStatus(fiber.StatusCreated)
}

func searchEstates(c *fiber.Ctx) error {
	conditions := make([]string, 0)
	params := make([]interface{}, 0)

	qs := UnsafeString(c.Context().URI().QueryString())
	if r, found := estateCache.Get(qs); found {
		return JSONBlob(c, fiber.StatusOK, r.([]byte))
	}

	if c.Query("doorHeightRangeId") != "" {
		doorHeight, err := getRange(estateSearchCondition.DoorHeight, c.Query("doorHeightRangeId"))
		if err != nil {
			log.Printf("doorHeightRangeID invalid, %v : %v", c.Query("doorHeightRangeId"), err)
			return c.SendStatus(fiber.StatusBadRequest)
		}
		conditions = append(conditions, "door_height_range = ?")
		params = append(params, doorHeight.ID)
	}

	if c.Query("doorWidthRangeId") != "" {
		doorWidth, err := getRange(estateSearchCondition.DoorWidth, c.Query("doorWidthRangeId"))
		if err != nil {
			log.Printf("doorWidthRangeID invalid, %v : %v", c.Query("doorWidthRangeId"), err)
			return c.SendStatus(fiber.StatusBadRequest)
		}
		conditions = append(conditions, "door_width_range = ?")
		params = append(params, doorWidth.ID)
	}

	if c.Query("rentRangeId") != "" {
		estateRent, err := getRange(estateSearchCondition.Rent, c.Query("rentRangeId"))
		if err != nil {
			log.Printf("rentRangeID invalid, %v : %v", c.Query("rentRangeId"), err)
			return c.SendStatus(fiber.StatusBadRequest)
		}
		conditions = append(conditions, "rent_range = ?")
		params = append(params, estateRent.ID)
	}

	if c.Query("features") != "" {
		for _, f := range strings.Split(c.Query("features"), ",") {
			conditions = append(conditions, "features like concat('%', ?, '%')")
			params = append(params, f)
		}
	}

	if len(conditions) == 0 {
		log.Printf("searchEstates search condition not found")
		return c.SendStatus(fiber.StatusBadRequest)
	}

	page, err := strconv.Atoi(c.Query("page"))
	if err != nil {
		log.Printf("Invalid format page parameter : %v", err)
		return c.SendStatus(fiber.StatusBadRequest)
	}

	perPage, err := strconv.Atoi(c.Query("perPage"))
	if err != nil {
		log.Printf("Invalid format perPage parameter : %v", err)
		return c.SendStatus(fiber.StatusBadRequest)
	}

	searchQuery := "SELECT id FROM estate WHERE "
	countQuery := "SELECT COUNT(*) FROM estate WHERE "
	searchCondition := strings.Join(conditions, " AND ")
	limitOffset := " ORDER BY popularity DESC, id ASC LIMIT ? OFFSET ?"
	//fmt.Printf("%s\n", searchCondition)
	var res types.EstateSearchResponse
	countKey := searchCondition + UnsafeString(gotiny.Marshal(&params))
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

	ids := make([]int64, 0, perPage)
	paramsQ := append(params, perPage, page*perPage)
	err = db.Select(&ids, searchQuery+searchCondition+limitOffset, paramsQ...)
	if err != nil {
		if err == sql.ErrNoRows {
			return json.NewEncoder(c.Type("application/json").Status(fiber.StatusOK)).Encode(types.EstateSearchResponse{Count: 0, Estates: &[]types.Estate{}})
		}
		log.Printf("searchEstates DB execution error : %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
	}

	wg.Wait()
	if cntErr != nil {
		log.Printf("searchEstates DB execution error : %v", cntErr)
		return c.SendStatus(fiber.StatusInternalServerError)
	}

	arr := estatePool.Get().(*[]types.Estate)
	applyEstates(ids, arr)
	defer estatePool.Put(arr)
	res.Estates = arr
	b, _ := res.MarshalJSON()
	estateCache.Set(qs, b)

	return JSONBlob(c, fiber.StatusOK, b)
}

func getLowPricedEstate(c *fiber.Ctx) error {
	if r, found := estateCache.Get("getLowPricedEstate"); found {
		return JSONBlob(c, fiber.StatusOK, r.([]byte))
	}
	r, e, _ := sfGroup.Do("getLowPricedEstate", func() (interface{}, error) {

		ids := make([]int64, 0, Limit)
		query := `SELECT id FROM estate ORDER BY rent ASC, id ASC LIMIT ?`
		err := db.Select(&ids, query, Limit)
		if err != nil {
			if err != sql.ErrNoRows {
				log.Printf("getLowPricedEstate not found")
				return []byte("[]"), nil
			}
			log.Printf("getLowPricedEstate DB execution error : %v", err)
			return nil, err
		}

		arr := estatePool.Get().(*[]types.Estate)
		applyEstates(ids, arr)
		defer estatePool.Put(arr)
		b, _ := types.EstateListResponse{Estates: arr}.MarshalJSON()
		estateCache.Set("getLowPricedEstate", b)

		return b, nil
	})
	if e != nil {
		return c.SendStatus(fiber.StatusInternalServerError)
	}
	return JSONBlob(c, fiber.StatusOK, r.([]byte))
}

func searchRecommendedEstateWithChair(c *fiber.Ctx) error {
	id, err := strconv.Atoi(c.Params("id"))
	if err != nil {
		log.Printf("Invalid format searchRecommendedEstateWithChair id : %v", err)
		return c.SendStatus(fiber.StatusBadRequest)
	}

	if r, found := estateCache.Get("recom" + c.Params("id")); found {
		//log.Printf("recom hit")
		return JSONBlob(c, fiber.StatusOK, r.([]byte))
	}

	chair, ok := chairObjCache.Get(int64(id))
	if !ok {
		return c.SendStatus(fiber.StatusBadRequest)
	}

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
	ids := make([]int64, 0, Limit)
	err = db.Select(&ids, query, minLen, midLen, midLen, minLen, Limit)
	if err != nil {
		if err == sql.ErrNoRows {
			return json.NewEncoder(c.Type("application/json").Status(fiber.StatusOK)).Encode(types.EstateListResponse{&[]types.Estate{}})
		}
		log.Printf("Database execution error : %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
	}

	arr := estatePool.Get().(*[]types.Estate)
	applyEstates(ids, arr)
	defer estatePool.Put(arr)
	b, _ := types.EstateListResponse{Estates: arr}.MarshalJSON()
	estateCache.Set("recom"+c.Params("id"), b)

	return JSONBlob(c, fiber.StatusOK, b)
}

func searchEstateNazotte(c *fiber.Ctx) error {
	coordinates := types.Coordinates{}
	err := c.BodyParser(&coordinates)
	if err != nil {
		log.Printf("post search estate nazotte failed : %v", err)
		return c.SendStatus(fiber.StatusBadRequest)
	}

	if len(coordinates.Coordinates) == 0 {
		return c.SendStatus(fiber.StatusBadRequest)
	}

	po := coordinates.CoordinatesToText()
	query := fmt.Sprintf("SELECT id FROM estate FORCE INDEX(idx_point) WHERE ST_Contains(ST_PolygonFromText('POLYGON((%s))'), point) ORDER BY popularity DESC, id ASC LIMIT 50", po)
	ids := []int64{}
	err = db.Select(&ids, query)

	var re types.EstateSearchResponse
	arr := estatePool.Get().(*[]types.Estate)
	applyEstates(ids, arr)
	defer estatePool.Put(arr)
	re.Estates = arr
	re.Count = int64(len(ids))

	s := bytesPool.Get().([]byte)
	defer bytesPool.Put(s)
	w := jwriter.Writer{}
	re.MarshalEasyJSON(&w)
	return c.Type("application/json").Status(fiber.StatusOK).Send(w.Buffer.BuildBytes(s))
}

func postEstateRequestDocument(c *fiber.Ctx) error {
	m := map[string]interface{}{}
	if err := c.BodyParser(&m); err != nil {
		log.Printf("post request document failed : %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
	}

	_, ok := m["email"].(string)
	if !ok {
		log.Printf("post request document failed : email not found in request body")
		return c.SendStatus(fiber.StatusBadRequest)
	}

	id, err := strconv.Atoi(c.Params("id"))
	if err != nil {
		log.Printf("post request document failed : %v", err)
		return c.SendStatus(fiber.StatusBadRequest)
	}

	if _, ok := estateObjCache.Get(int64(id)); ok {
		return c.SendStatus(fiber.StatusOK)
	}

	estate := types.Estate{}
	query := `SELECT * FROM estate WHERE id = ?`
	err = db.Get(&estate, query, id)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.SendStatus(fiber.StatusNotFound)
		}
		log.Printf("postEstateRequestDocument DB execution error : %v", err)
		return c.SendStatus(fiber.StatusInternalServerError)
	}

	return c.SendStatus(fiber.StatusOK)
}

func getEstateSearchCondition(c *fiber.Ctx) error {
	return JSONBlob(c, fiber.StatusOK, estateSearchConditionJSON)
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
