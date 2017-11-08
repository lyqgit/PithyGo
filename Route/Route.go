package Route

import (
	"net/http"
	"PithyGo/Control"
	"strings"
)

type route struct{
	normalUrl map[string]Func
	parameterUrl map[string]Func
}

var Mux = http.NewServeMux()
var rter *route

func init(){
	rter = new(route)
	rter.init()
	Mux.Handle("/",rter)
}

func(r *route)init(){
	r.normalUrl = make(map[string]Func)
	r.parameterUrl = make(map[string]Func)
}

func RouteStatic(pattern string,root string){
	Mux.Handle(pattern,http.StripPrefix(pattern,http.FileServer(http.Dir(root))))
}



func Route(param string,con Func){
	if strings.Contains(param,":"){
		rter.parameterUrl[param] = con
	}else{
		rter.normalUrl[param] = con
	}
	
}

type Func func()


func(ro *route)ServeHTTP(w http.ResponseWriter,r *http.Request){

	var url string = r.URL.String()

	if strings.Contains(url,":"){
		for k,v := range ro.parameterUrl{
			if len(strings.Split(k,"/")) == len(strings.Split(url,"/")){			
				v()
			}else{
				continue
			}
		}
	}else{
		for k,v := range ro.normalUrl{
			if k == url{
				v()
			}else{
				continue
			}
		}
	}
	
}
