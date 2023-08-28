package main

//go:generate msgp -tests=false

// ClickhouseRequest - request struct for queue
type ClickhouseRequest struct {
	Params   string `msg:"Params"`
	Query    string `msg:"Query"`
	Content  string `msg:"Content"`
	Count    int    `msg:"Count"`
	IsInsert bool   `msg:"IsInsert"`
}
