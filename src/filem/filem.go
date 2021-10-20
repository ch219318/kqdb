package filem

import (
	"encoding/binary"
	"io/ioutil"
	"kqdb/src/global"
	"log"
	"os"
	"path/filepath"
	"strings"
)

//文件管理模块 fileName带后缀
func init() {
	global.InitLog()
}

const (
	SIZE_B              int = 1
	SIZE_K                  = 1024 * SIZE_B
	SIZE_M                  = 1024 * SIZE_K
	SIZE_G                  = 1024 * SIZE_M
	DATA_FILE_INIT_SIZE     = 9 * SIZE_M //数据文件初始大小
	PageSize                = 8 * SIZE_K //分页大小
	DataFileSuf             = "data"     //数据文件扩展名
	FrameFileSuf            = "frm"      //结构文件扩展名
)

//定义列数据类型枚举值
type FileType int

const (
	FileTypeData FileType = 1 + iota
	FileTypeFrame
)

//===========================

//key为filePath
var filesMap = initFilesMap()

func initFilesMap() map[string]*FileHandler {
	filesMap := make(map[string]*FileHandler)

	//获取所有schema
	dirNames := ListDir(global.DataDir)
	for _, dirName := range dirNames {

		dirPath := filepath.Join(global.DataDir, dirName)
		//处理frm文件
		frmFileNames := ListFile(dirPath, FrameFileSuf)
		for _, fileName := range frmFileNames {
			fileP := filepath.Join(dirPath, fileName)
			frameFileHandler := openFile(fileP)
			filesMap[fileP] = frameFileHandler
		}
		//处理data文件
		dataFileNames := ListFile(dirPath, DataFileSuf)
		for _, fileName := range dataFileNames {
			fileP := filepath.Join(dirPath, fileName)
			dataFileHandler := openFile(fileP)
			filesMap[fileP] = dataFileHandler
		}
	}

	return filesMap
}

func CloseFilesMap() {
	for fileP := range filesMap {
		fileHandler := filesMap[fileP]
		fileHandler.Close()
	}
}

//获取指定目录下的所有目录
func ListDir(dirPth string) (dirNames []string) {
	dirNames = make([]string, 0, 10)
	dir, err := ioutil.ReadDir(dirPth)
	if err != nil {
		log.Panic(err)
	}
	for _, fi := range dir {
		if fi.IsDir() {
			dirNames = append(dirNames, fi.Name())
		} else {
			continue
		}
	}
	return
}

//获取指定目录下的所有文件，不进入下一级目录搜索，可以匹配后缀过滤。
func ListFile(dirPth string, suffix string) (fileNames []string) {
	fileNames = make([]string, 0, 10)
	dir, err := ioutil.ReadDir(dirPth)
	if err != nil {
		log.Panic(err)
	}
	suffix = strings.ToUpper(suffix) //忽略后缀匹配的大小写
	for _, fi := range dir {
		if fi.IsDir() { // 忽略目录
			continue
		}
		if strings.HasSuffix(strings.ToUpper(fi.Name()), "."+suffix) { //匹配文件
			fileNames = append(fileNames, fi.Name())
		}
	}
	return
}

//===========================

type FileHandler struct {
	fileType  FileType
	filePath  string
	File      *os.File
	TotalPage int
}

func CreateDataFile(fileP string) {
	log.Printf("开始创建数据文件:%s\n", fileP)

	file, err := os.Create(fileP)
	defer file.Close()
	if err != nil {
		log.Panicf("创建数据文件:%s 失败\n", fileP)
	}

	//文件头page
	metabytes := make([]byte, PageSize)
	file.Write(metabytes)

	//初始化data-page
	pageBuf := make([]byte, PageSize)
	pageLower := uint16(24)
	pageUpper := uint16(PageSize - 1)
	binary.BigEndian.PutUint16(pageBuf[2:4], pageLower)
	binary.BigEndian.PutUint16(pageBuf[4:6], pageUpper)

	//写入初始化data-page
	num := DATA_FILE_INIT_SIZE/PageSize - 1
	for i := 0; i < int(num); i++ {
		file.Write(pageBuf)
	}

	//FilesMap处理
	dataFileHandler := openFile(fileP)
	filesMap[fileP] = dataFileHandler
	frmFileP := strings.TrimSuffix(fileP, DataFileSuf) + FrameFileSuf
	frameFileHandler := openFile(frmFileP)
	filesMap[frmFileP] = frameFileHandler

	//buffer_pool处理
	addFileToPool(fileP)

	log.Printf("创建数据文件:%s成功\n", fileP)
}

func openFile(filePath string) (fileHandler *FileHandler) {
	fileType := getFileType(filePath)

	datafile, err := os.OpenFile(filePath, os.O_RDWR, os.ModePerm)
	if err == nil {
		fileHandler = new(FileHandler)
		fileHandler.filePath = filePath
		fileHandler.fileType = fileType
		fileHandler.File = datafile

		if fileType == FileTypeData {
			fileInfo, err := datafile.Stat()
			if err != nil {
				log.Panicln(err)
			}
			if int(fileInfo.Size())%PageSize != 0 {
				log.Panicln("数据文件长度出错")
			}
			fileHandler.TotalPage = int(fileInfo.Size()) / PageSize
		}

	} else {
		log.Panicln(err)
	}
	log.Printf("打开文件：%s\n", fileHandler.filePath)
	return
}

func getFileType(filePath string) FileType {
	if strings.HasSuffix(filePath, FrameFileSuf) {
		return FileTypeFrame
	}
	if strings.HasSuffix(filePath, DataFileSuf) {
		return FileTypeData
	}
	log.Panicln("不支持的类型")
	return 0
}

func GetFile(fileP string) *FileHandler {
	fileHandler := filesMap[fileP]
	if fileHandler == nil {
		log.Panicln(fileP, "文件不存在")
	}
	return fileHandler
}

func (fh *FileHandler) Close() {
	err := fh.File.Close()
	if err != nil {
		log.Panicln(err)
	}
	log.Printf("关闭文件：%s\n", fh.filePath)
}

//先从bufferPool获取，然后在从硬盘中取
func (fh *FileHandler) GetPage(pageNum int) *Page {
	if fh.fileType != FileTypeData {
		log.Panic("不是数据文件")
	}

	//从buffer_pool中获取page
	var page *Page
	pageList := bufferPool[fh.filePath].PageList
	for e := pageList.Front(); e != nil; e = e.Next() {
		p := e.Value.(*Page)
		if p.PageNum == pageNum {
			page = p
			break
		}
	}

	//如果page链上没有，从脏链上获取
	if page == nil {
		dirtyPageList := bufferPool[fh.filePath].DirtyPageList
		for e := dirtyPageList.Front(); e != nil; e = e.Next() {
			p := e.Value.(*Page)
			if p.PageNum == pageNum {
				page = p
				break
			}
		}
	}

	//如果buffer_pool中page不存在，从文件中获取page，并放入buffer_pool
	if page == nil {
		page = fh.readPageFromDisk(pageNum)
		//放入buffer_pool
		pageList.PushBack(page)
	}

	return page
}

//分配新page
func (fh *FileHandler) AllocatePage() *Page {
	//生成page
	page := new(Page)
	page.PageNum = fh.TotalPage
	page.FilePath = fh.filePath
	page.Lower = 24
	page.Upper = PageSize - 1
	page.Items = make([]*Item, 0)
	page.Content = make([]byte, 0)

	//放入bufferPool的dirty链
	bufferPool[fh.filePath].DirtyPageList.PushBack(page)

	//FileHandler属性修改
	fh.TotalPage++
	return page
}

//从硬盘获取page
func (fh *FileHandler) readPageFromDisk(pageNum int) *Page {
	if fh.fileType != FileTypeData {
		log.Panic("不是数据文件")
	}

	bytes := make([]byte, PageSize)
	offset := pageNum * PageSize
	_, err := fh.File.ReadAt(bytes, int64(offset))
	if err != nil {
		log.Panic(err)
	}
	page := new(Page)
	page.UnMarshal(bytes, pageNum, fh.filePath)
	return page
}

func (fh *FileHandler) MarkDirty(pageNum int) {
	dirtyPageList := bufferPool[fh.filePath].DirtyPageList
	pageList := bufferPool[fh.filePath].PageList

	var resultPage *Page = nil

	for e := dirtyPageList.Front(); e != nil; e = e.Next() {
		dirtyPage := e.Value.(*Page)
		if pageNum == dirtyPage.PageNum {
			resultPage = dirtyPage
			break
		}
	}

	//当dirty链上不存在时，从page链上查找
	if resultPage == nil {
		for e := pageList.Front(); e != nil; e = e.Next() {
			page := e.Value.(*Page)
			if pageNum == page.PageNum {
				resultPage = page
				//从page链中删除当前page
				pageList.Remove(e)
				break
			}

		}
	}

	if resultPage != nil {
		//加入dirty链
		dirtyPageList.PushBack(resultPage)
	} else {
		log.Panicln("bufferPool不存在page：", pageNum)
	}

}
