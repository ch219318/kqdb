package pf

import (
	"encoding/binary"
	"log"
	"os"
)

const (
	SIZE_B                         int64 = 1
	SIZE_K                               = 1024 * SIZE_B
	SIZE_M                               = 1024 * SIZE_K
	SIZE_G                               = 1024 * SIZE_M
	DATA_FILE_INIT_SIZE                  = 9 * SIZE_M //数据文件初始大小
	DATA_FILE_HEADER_SIZE                = 8 * SIZE_M //数据文件文件头大小
	DATA_FILE_HEADER_METAINFO_SIZE       = 1 * SIZE_K //数据文件头里元信息大小
	DATA_FILE_PAGE_SIZE                  = 4 * SIZE_K //数据文件分页大小
	DATA_FILE_EXT_NAME                   = "myd"      //数据文件扩展名
	DATA_FILE_BASE_PATH                  = "../data/"
	FRAME_FILE_EXT_NAME                  = "frm" //结构文件扩展名

)

type FileHandle struct {
	Path     string //相对于data文件夹
	FileName string
	File     os.File
}

type PageHandle struct {
	PageNodeId int //
}

func CreateDataFile(name string) (erro error) {
	log.Printf("开始创建数据文件:%s.%s\n", name, DATA_FILE_EXT_NAME)
	file, err := os.Create(DATA_FILE_BASE_PATH + name + "." + DATA_FILE_EXT_NAME)
	defer file.Close()
	buf := make([]byte, 1024)
	if err == nil {
		max := DATA_FILE_INIT_SIZE / 1024
		log.Printf("max:%v\n", max)
		for i := int64(0); i < max; i++ {
			file.Write(buf)
		}
	} else {
		log.Printf("创建数据文件:%s.%s失败\n", name, DATA_FILE_EXT_NAME)
		return err
	}
	log.Printf("创建数据文件:%s.%s成功\n", name, DATA_FILE_EXT_NAME)
	return erro
}

func OpenDataFile(path string, name string) (fileHandle FileHandle, err error) {
	address := DATA_FILE_BASE_PATH + Path + "/" + name
	datafile, err1 := os.OpenFile(address, os.O_RDWR|os.O_APPEND, 0666)
	if err1 == nil {
		fileHandle = FileHandle{Path: path, FileName: name, File: datafile}
	} else {
		return fileHandle, err1
	}
	return fileHandle, err
}

func CloseDataFile(fh *FileHandle) error {
	err1 := fh.File.Close()
	return err1
}

func (fh *FileHandle) AddData(bytes []byte) (err error) {
	length := len(bytes)
	if int64(length) <= DATA_FILE_PAGE_SIZE {
		//更新文件头信息

		//添加数据部分
		address := DATA_FILE_BASE_PATH + fh.Path + "/" + fh.FileName
		file, err1 := os.OpenFile(address, os.O_WRONLY|os.O_APPEND, 0666)
		defer file.Close()
		if err1 == nil {
			content := make([]byte, DATA_FILE_PAGE_SIZE)
			copy(content, bytes)
			// // 查找文件末尾的偏移量
			// off, _ := file.Seek(0, os.SEEK_END)
			// // 从末尾的偏移量开始写入内容
			// n, err2 := file.WriteAt(content, off)
			n, err2 := file.Write(content)
			log.Printf("添加数据成功%d:%v", n, err2)
		} else {
			return err1
		}
	} else {

	}
	return err
}

type MetaInfo struct {
	CurPageId     int //最后一个page的id
	CurSeqId      int //最后一个序列id
	CurNodePageId int //当前node所在pageid
}

//获取文件头中node表元信息
func (fh *FileHandle) GetMetaInfo() (mi MetaInfo) {
	file := fh.File
	content := make([]byte, DATA_FILE_HEADER_METAINFO_SIZE)
	n, err2 := file.Read(content)
	log.Printf("获取数据成功%d:%v\n", n, err2)
	mi.CurPageId = int(binary.BigEndian.Uint32(content[0:4]))
	mi.CurSeqId = int(binary.BigEndian.Uint32(content[4:8]))
	mi.CurNodePageId = int(binary.BigEndian.Uint32(content[8:12]))
	log.Printf("mi:%v\n", mi)
	return mi
}

//保存文件头中node表元信息
func (fh *FileHandle) SaveMetaInfo() (mi MetaInfo) {
}

//初始化文件头中node表元信息
func (fh *FileHandle) InitMetaInfo() (mi MetaInfo) {
}
