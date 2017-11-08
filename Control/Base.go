package Control

import (
	"net/http"
	"html/template"
)

type ControInter interface{
	Init(w http.ResponseWriter,r *http.Request)
	Assign()
	Export()
	GetParameter()
	Destruct(w http.ResponseWriter,r *http.Request)
	Fetch(html string)ControInter
}

type Controller struct{
	template string
	outData string
	tempData interface{}
	Pithy_Request map[string][]string
	RequestMethod string
}

func(c *Controller)Fetch(html string){
	c.template = html
}

func(c *Controller)Assign(data interface{}){
	c.tempData = data
}

func(c *Controller)Export(data string){
	c.outData = data
}

func(c *Controller)GetParameter(key string)[]string{
	return c.Pithy_Request[key]
}

func(c *Controller)Init(w http.ResponseWriter,r *http.Request){
	c.template = ""
	c.RequestMethod = r.Method
	c.Pithy_Request = r.Form
}
func(c *Controller)Destruct(w http.ResponseWriter,r *http.Request){
	if c.template == ""{
		w.Write([]byte(c.outData))
	}else{
		t,err := template.ParseFiles(c.template)
		check(err)
		w.Header().Set("content-type","text/html; charset=UTF-8")
		t.Execute(w,nil)
	}
}

func check(err error){
	if err != nil{
		panic(err)
	}
}
