package Control

import (
	"net/http"
	"html/template"
)

type ControInter interface{
	Init(w http.ResponseWriter,r *http.Request)
	Assign()
	Request()
	Export()
	GetForms(key string)[]string
	GetForm(key string)string
	Destruct(w http.ResponseWriter,r *http.Request)
	Fetch(html string)ControInter
}

type Controller struct{
	template string
	outData string
	tempData interface{}
	pithy_Request map[string][]string
	requestMethod string
	url string
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

func(c *Controller)Request(){
	
}

func(c *Controller)GetForms(key string)[]string{
	return c.pithy_Request[key]
}

func(c *Controller)GetForm(key string)string{
	return c.pithy_Request[key][0]
}

func(c *Controller)Init(w http.ResponseWriter,r *http.Request){
	c.template = ""
	c.requestMethod = r.Method
	c.pithy_Request = r.Form
	c.tempData = nil
	c.url = r.URL.String()
}
func(c *Controller)Destruct(w http.ResponseWriter,r *http.Request){
	if c.template == ""{
		w.Write([]byte(c.outData))
	}else{
		t,err := template.ParseFiles(c.template)
		check(err)
		w.Header().Set("content-type","text/html; charset=UTF-8")
		t.Execute(w,c.tempData)
	}
}

func check(err error){
	if err != nil{
		panic(err)
	}
}
