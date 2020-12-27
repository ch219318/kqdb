package pf

import (
	"encoding/binary"
	"log"
	"os"
)

//文件管理模块
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
	NODE_SIZE                            = 8 * SIZE_B
)

type FileHandle struct {
	Path     string //相对于data文件夹
	FileName string
	File     *os.File
}

type PageHandle struct {
	PageNodeId int //
}

func CreateDataFile(name string) (erro error) {
	log.Printf("开始创建数据文件:%s.%s\n", name, DATA_FILE_EXT_NAME)
	file, err := os.Create(DATA_FILE_BASE_PATH + name + "." + DATA_FILE_EXT_NAME)
	defer file.Close()
	//设置元信息
	metabytes := make([]byte, 1024)
	metabytes[2] = 0x08
	metabytes[3] = 0x01
	metabytes[7] = 0x01
	metabytes[11] = 0x02
	file.Write(metabytes)
	buf := make([]byte, 1024)
	if err == nil {
		max := DATA_FILE_INIT_SIZE/1024 - 1
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

func OpenDataFile(path string, name string) (fileHandle *FileHandle, err error) {
	address := DATA_FILE_BASE_PATH + path + "/" + name
	datafile, err1 := os.OpenFile(address, os.O_RDWR|os.O_APPEND, os.ModePerm)
	if err1 == nil {
		file := FileHandle{Path: path, FileName: name, File: datafile}
		fileHandle = &file
	} else {
		return fileHandle, err1
	}
	log.Printf("fileHandle指针地址%p\n", fileHandle)
	return fileHandle, err
}

func CloseDataFile(fh *FileHandle) error {
	err1 := fh.File.Close()
	return err1
}

func (fh *FileHandle) AddData(bytes []byte) (err error) {
	length := len(bytes)
	file := fh.File
	//获取文件头元信息
	mi := fh.GetMetaInfo()

	if int64(length) <= DATA_FILE_PAGE_SIZE {
		//添加数据部分
		content := make([]byte, DATA_FILE_PAGE_SIZE)
		copy(content, bytes)
		// // 查找文件末尾的偏移量
		// off, _ := file.Seek(0, os.SEEK_END)
		// // 从末尾的偏移量开始写入内容
		// n, err2 := file.WriteAt(content, off)
		off := (int64(mi.CurPageId) - 1) * (DATA_FILE_PAGE_SIZE)
		n, err1 := file.WriteAt(content, off)
		if err1 != nil {
			return err1
		}
		log.Printf("添加数据成功%d:%v", n, err1)
		//更新文件头信息
		node := make([]byte, 8)
		pageIdOfNode := make([]byte, 4)
		binary.BigEndian.PutUint32(pageIdOfNode, uint32(mi.CurPageId))
		seqIdOfNode := make([]byte, 4)
		binary.BigEndian.PutUint32(seqIdOfNode, uint32(mi.CurSeqId))
		copy(node, pageIdOfNode)
		node[4] = seqIdOfNode[1]
		node[5] = seqIdOfNode[2]
		node[6] = seqIdOfNode[3]
		node[7] = 0x02 //00000010，倒数第一位表示数据还是地址，第二位表示node是否有效
		file.WriteAt(node, DATA_FILE_PAGE_SIZE+(int64(mi.CurSeqId)-1)*NODE_SIZE)
		//更新文件头元信息
		mi.CurPageId = mi.CurPageId + 1
		mi.CurSeqId = mi.CurSeqId + 1
		err2 := fh.SaveMetaInfo(mi)
		if err2 != nil {
			return err2
		}
	} else {

	}
	return err
}

func (fh *FileHandle) GetData() (bytes []byte, err error) {
}

type MetaInfo struct {
	CurPageId     int //下一个待分配page的id，以2049开始，初始值为2049
	CurSeqId      int //下一个待分配序列id，以1开始，初始值为1
	CurNodePageId int //最末尾node信息所在pageid，以2开始，初始值为2
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
func (fh *FileHandle) SaveMetaInfo(mi MetaInfo) (err error) {
	file := fh.File
	CurPageIdBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(CurPageIdBytes, uint32(mi.CurPageId))
	CurSeqIdBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(CurSeqIdBytes, uint32(mi.CurSeqId))
	CurNodePageId := make([]byte, 4)
	binary.BigEndian.PutUint32(CurNodePageId, uint32(mi.CurNodePageId))
	//slice1的长度为12，写入成功为8，而且err1为nil，找不出原因
	// slice := append(CurPageIdBytes, CurSeqIdBytes...)
	// slice1 := append(slice, CurNodePageId...)
	// log.Println(len(slice1))
	// n, err1 := file.WriteAt(slice, 10)
	file.WriteAt(CurPageIdBytes, 0)
	file.WriteAt(CurSeqIdBytes, 4)
	file.WriteAt(CurNodePageId, 8)
	return err
}
