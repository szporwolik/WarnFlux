package httpwebhook
import("context";"io";"net/http";"net/http/httptest";"testing")
func TestReview7RedirectLosesAlert(t *testing.T){
 type seen struct{method string;bytes int}
 got:=make(chan seen,1)
 srv:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
  if r.URL.Path=="/hook"{http.Redirect(w,r,"/login",http.StatusFound);return}
  b,_:=io.ReadAll(r.Body);got<-seen{r.Method,len(b)};w.WriteHeader(http.StatusOK)
 }))
 defer srv.Close()
 p,err:=New(node(t,map[string]any{"url":srv.URL+"/hook"}));if err!=nil{t.Fatal(err)}
 err=p.Execute(context.Background(),hazardRequest())
 request:=<-got
 if err==nil{t.Fatalf("delivery reported success after redirect: method=%s body=%d bytes",request.method,request.bytes)}
}
