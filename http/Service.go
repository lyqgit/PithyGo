package http

import (
	"net/http"
	"PithyGo/Route"
)

func RunOn(){
	Server := &http.Server{
		Addr:":8080"
	}
}
