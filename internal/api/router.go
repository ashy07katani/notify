package api

import (
	"notify/internal/migrations"
	"notify/internal/service"
	"os"

	"github.com/gin-gonic/gin"
)

type Router struct {
	router *gin.Engine
	svc    *service.Service
}

func getRouter() *Router {
	r := gin.Default()
	s := service.InitService()
	return &Router{
		router: r,
		svc:    s,
	}
}

func (r *Router) defineRoute(router *gin.Engine) {
	router.POST("/notify", r.svc.PostNotify)
}

func Start() {
	migrations.CreateTopic(os.Getenv("KAFKA_TOPIC_RAW"))

	r := getRouter()
	r.defineRoute(r.router)
	r.router.Run(os.Getenv("API_PORT"))
}
