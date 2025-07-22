package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

type OHLCV struct {
	ID         int     `json:"id"`
	Unixtime   int     `json:"unixtime"`
	Date       string  `json:"date"`
	Time       string  `json:"time"`
	PriceOpen  float64 `json:"priceopen"`
	PriceHigh  float64 `json:"pricehigh"`
	PriceLow   float64 `json:"pricelow"`
	PriceClose float64 `json:"priceclose"`
	DealVolume float64 `json:"dealvolume"`
}

type App struct {
	DB *sql.DB
}

func (a *App) Initialize() error {
	connStr := "user=postgres dbname=postgres password=J,@CC2@oCa{R'^O] host=34.142.206.64 sslmode=disable"
	var err error
	a.DB, err = sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}

	a.DB.SetMaxOpenConns(20)
	a.DB.SetMaxIdleConns(10)
	return nil
}

func (a *App) GetOHLCV(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	symbol := vars["symbol"]
	startDate := r.URL.Query().Get("start_date")
	endDate := r.URL.Query().Get("end_date")

	query := "SELECT id, unixtime, date, time, priceopen, pricehigh, pricelow, priceclose, dealvolume FROM btcusdt1m WHERE symbol = $1"
	args := []interface{}{symbol}

	if startDate != "" && endDate != "" {
		query += " AND date BETWEEN $2 AND $3"
		args = append(args, startDate, endDate)
	}

	query += " LIMIT 1"
	rows, err := a.DB.Query(query, args...)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to query data: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var ohlcvData []OHLCV
	for rows.Next() {
		var o OHLCV
		if err := rows.Scan(&o.ID, &o.Unixtime, &o.Date, &o.Time, &o.PriceOpen, &o.PriceHigh, &o.PriceLow, &o.PriceClose, &o.DealVolume); err != nil {
			http.Error(w, fmt.Sprintf("failed to scan data: %v", err), http.StatusInternalServerError)
			return
		}
		ohlcvData = append(ohlcvData, o)
	}

	if len(ohlcvData) == 0 {
		http.Error(w, "no data found for symbol", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ohlcvData)
}

func main() {
	app := &App{}
	if err := app.Initialize(); err != nil {
		log.Fatal(err)
	}
	defer app.DB.Close()

	router := mux.NewRouter()
	router.HandleFunc("/ohlcv/{symbol}", app.GetOHLCV).Methods("GET")

	fmt.Println("Server running on port 8080")
	log.Fatal(http.ListenAndServe(":8080", router))
}
