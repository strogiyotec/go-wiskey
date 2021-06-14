package http

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"sync"
	. "wiskey/pkg"
)

type Value struct {
	Value string `json:"value" binding:"required"`
}

func Start(lsm *LsmTree) {
	var m sync.Mutex
	router := gin.New()
	router.DELETE("/:key", func(c *gin.Context) {
		m.Lock()
		key := c.Param("key")
		err := lsm.Delete([]byte(key))
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"error": err.Error()})
		} else {
			c.Status(http.StatusAccepted)
		}
		m.Unlock()
	})
	router.GET("/fetch/:key", func(c *gin.Context) {
		m.Lock()
		key := c.Param("key")
		value, found := lsm.Get([]byte(key))
		if found {
			c.JSON(http.StatusOK, gin.H{"value": string(value)})
		} else {
			c.Status(http.StatusNotFound)
		}
		m.Unlock()
	})
	router.POST("/:key", func(c *gin.Context) {
		m.Lock()
		var json Value
		key := c.Param("key")
		if err := c.ShouldBindJSON(&json); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		entry := NewEntry([]byte(key), []byte(json.Value))
		err := lsm.Put(&entry)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		} else {
			c.Status(http.StatusAccepted)
		}
		m.Unlock()
	})

	err := router.Run(":8080")
	if err != nil {
		panic(err)
	}
}
