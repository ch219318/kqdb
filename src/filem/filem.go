package filem

import (
	"encoding/binary"
	"errors"
	"io/ioutil"
	"kqdb/src/global"
	"log"
	"os"
	"path/filepath"
	"strings"
)

//文件管理模块
func init() {
	global.InitLog()
}

const (
	SIZE_B                         int = 1
	SIZE_K                             = 1024 * SIZE_B
	SIZE_M                             = 1024 * SIZE_K
	SIZE_G                             = 1024 * SIZE_M
	DATA_FILE_INIT_SIZE                = 9 * SIZE_M //数据文件初始大小
	DATA_FILE_HEADER_SIZE              = 8 * SIZE_M //数据文件文件头大小
	DATA_FILE_HEADER_METAINFO_SIZE     = 1 * SIZE_K //数据文件头里元信息大小
	PageSize                           = 8 * SIZE_K //分页大小
	DataFileSuf                        = "data"     //数据文件扩展名
	FrameFileSuf                       = "frm"      //结构文件扩展名
	NODE_SIZE                          = 8 * SIZE_B
)

//定义列数据类型枚举值
type FileType int

const (
	FileTypeData FileType = 1 + iota
	FileTypeFrame
)

var FilesMap = initFilesMap()

//key为schema和table。0位置为frm文件，1位置为data文件
func initFilesMap() map[string]map[string][2]*FileHandler {
	filesMap := make(map[string]map[string][2]*FileHandler)

	//获取所有schema
	dirNames, err := ListDir(global.DataDir)
	if err != nil {
		log.Fatal(err)
	}
	for _, dirName := range dirNames {
		schemaName := dirName

		dirPath := filepath.Join(global.DataDir, dirName)
		fileNames, err := ListFile(dirPath, FrameFileSuf)
		if err != nil {
			log.Fatal(err)
		}

		fileMap := make(map[string][2]*FileHandler)
		for _, fileName := range fileNames {
			tableName := strings.TrimSuffix(fileName, "."+FrameFileSuf)

			frameFileHandler, err := openFile(FileTypeFrame, schemaName, tableName)
			if err != nil {
				log.Fatal(err)
			}
			dataFileHandler, err := openFile(FileTypeData, schemaName, tableName)
			if err != nil {
				log.Fatal(err)
			}
			fileMap[tableName] = [2]*FileHandler{frameFileHandler, dataFileHandler}
		}

		filesMap[schemaName] = fileMap
	}

	return filesMap
}

//获取指定目录下的所有目录
func ListDir(dirPth string) (dirNames []string, err error) {
	dirNames = make([]string, 0, 10)
	dir, err := ioutil.ReadDir(dirPth)
	if err != nil {
		return nil, err
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
func ListFile(dirPth string, suffix string) (fileNames []string, err error) {
	fileNames = make([]string, 0, 10)
	dir, err := ioutil.ReadDir(dirPth)
	if err != nil {
		return nil, err
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

type FileHandler struct {
	fileType FileType
	Path     string //相对于data文件夹
	FileName string
	File     *os.File
}

type PageHandler struct {
	PageNodeId int //
}

func CreateDataFile(fileName string) error {
	log.Printf("开始创建数据文件:%s.%s\n", fileName, DataFileSuf)

	dataPath := filepath.Join(global.DataDir, global.DefaultSchemaName, fileName+"."+DataFileSuf)
	file, err := os.Create(dataPath)
	defer file.Close()
	if err != nil {
		log.Printf("创建数据文件:%s.%s失败\n", fileName, DataFileSuf)
		return err
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

	log.Printf("创建数据文件:%s.%s成功\n", fileName, DataFileSuf)
	return nil
}

func openFile(fileType FileType, schemaName string, tableName string) (fileHandle *FileHandler, err error) {
	suf := ""
	switch fileType {
	case FileTypeData:
		suf = DataFileSuf
	case FileTypeFrame:
		suf = FrameFileSuf
	default:
		log.Fatalln("不支持的文件类型:", fileType)
	}

	address := filepath.Join(global.DataDir, schemaName, tableName+"."+suf)
	datafile, err := os.OpenFile(address, os.O_RDWR, os.ModePerm)
	if err == nil {
		fileHandle = new(FileHandler)
		fileHandle.Path = schemaName
		fileHandle.FileName = tableName
		fileHandle.fileType = fileType
		fileHandle.File = datafile
	} else {
		log.Fatalln(err)
	}
	log.Printf("打开文件：%s/%s.%s\n", fileHandle.Path, fileHandle.FileName, suf)
	return
}

func (fh *FileHandler) Close() error {
	err := fh.File.Close()
	suf := ""
	switch fh.fileType {
	case FileTypeData:
		suf = DataFileSuf
	case FileTypeFrame:
		suf = FrameFileSuf
	default:
		log.Fatalln("不支持的文件类型:", fh.fileType)
	}
	log.Printf("关闭文件：%s/%s.%s\n", fh.Path, fh.FileName, suf)
	return err
}

func (fh *FileHandler) GetPageData(pageNum int) ([]byte, error) {
	if fh.fileType == FileTypeData {
		bytes := make([]byte, PageSize)
		offset := pageNum * PageSize
		_, err := fh.File.ReadAt(bytes, int64(offset))
		if err != nil {
			return nil, err
		}
		return bytes, nil
	} else {
		return nil, errors.New("文件类型错误")
	}
}

//==============

func (fh *FileHandler) AddData(bytes []byte) (err error) {
	length := len(bytes)
	file := fh.File
	//获取文件头元信息
	mi := fh.GetMetaInfo()

	if length <= PageSize {
		//添加数据部分
		content := make([]byte, PageSize)
		copy(content, bytes)
		// // 查找文件末尾的偏移量
		// off, _ := file.Seek(0, os.SEEK_END)
		// // 从末尾的偏移量开始写入内容
		// n, err2 := file.WriteAt(content, off)
		off := (mi.CurPageId - 1) * PageSize
		n, err1 := file.WriteAt(content, int64(off))
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
		offset := PageSize + (mi.CurSeqId-1)*NODE_SIZE
		file.WriteAt(node, int64(offset))
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

//func (fh *FileHandler) GetData() (bytes []byte, err error) {
//}

type MetaInfo struct {
	CurPageId     int //下一个待分配page的id，以2049开始，初始值为2049
	CurSeqId      int //下一个待分配序列id，以1开始，初始值为1
	CurNodePageId int //最末尾node信息所在pageid，以2开始，初始值为2
}

//获取文件头中node表元信息
func (fh *FileHandler) GetMetaInfo() (mi MetaInfo) {
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
func (fh *FileHandler) SaveMetaInfo(mi MetaInfo) (err error) {
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
