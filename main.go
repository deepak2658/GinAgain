package main

import (
	"example/web-service-gin/kafka"
	"github.com/gin-gonic/gin"
	"net/http"
)

// album represents data about a record album.
type album struct {
	ProfileName string `json:"profileName"`
	ProfileHandle string `json:"profileHandle"`
	ProfileIconUrl string `json:"profileIconUrl"`
	TagLine string `json:"tagLine"`
	Followers string `json:"followers"`
}

type profileUrl struct{
	ProfileUrls string `json:"profile_urls"`
}

var profileUrls = []profileUrl{
	{},
}

// albums slice to seed record album data.
var albums = []album{
	{ProfileName: "God Cobra", ProfileHandle: "Blue Train", ProfileIconUrl: "John Coltrane", TagLine: "56.99",Followers:"32M"},
}

// getAlbums responds with the list of all albums as JSON.
func getAlbums(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, albums)
}

func getProfileUrls(c *gin.Context){
	c.IndentedJSON(http.StatusOK,profileUrls)

}
func postAlbums(c *gin.Context) {
	var newAlbum album

	// Call BindJSON to bind the received JSON to
	// newAlbum.
	if err := c.BindJSON(&newAlbum); err != nil {
		return
	}

	// Add the new album to the slice.
	albums = append(albums, newAlbum)
	c.IndentedJSON(http.StatusCreated, newAlbum)
}

func postUrls(c *gin.Context) {
	var newPofile profileUrl

	// Call BindJSON to bind the received JSON to
	// newAlbum.
	if err := c.BindJSON(&newPofile); err != nil {
		return
	}

	// Add the new album to the slice.
	kafka.Producer(newPofile.ProfileUrls)
	profileUrls = append(profileUrls, newPofile)
	c.IndentedJSON(http.StatusCreated, newPofile)
}

func main()  {
	server := gin.Default()

	server.GET("/all",getAlbums)
	server.POST("/add",postAlbums)

	kafka.StartKafka()

	server.GET("/urls/all",getProfileUrls)
	server.POST("/urls/a",postUrls)


	server.Run("localhost:8080")
}
