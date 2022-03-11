package main

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"hashbackup-go/file"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
)

const BUFFER = 1e6

var wg sync.WaitGroup

type Block struct {
	Index int
	Body  []byte
	Hash  []byte
}

type Options struct {
	Append bool
	Sync   bool
	Delete bool
	Quiet  bool
}

type Required struct {
	Source      string
	Destination string
}

type Routine struct {
	ID   int
	Name string
	End  chan bool
}

type FileCutter struct {
	Routine
}

type BlockHasher struct {
	Routine
}

func check(err error) {
	if err != nil {
		log.Println(err)
	}
}

func checkFatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// горутина нарезает файлы и возвращает sha файла
func (rt *Routine) fileCutter(name string, cmp chan bool, cs chan Block, b chan Block) string {
	var parts int
	var sum []byte

	target, err := os.OpenFile(name, os.O_RDONLY, os.ModePerm)
	checkFatal(err)
	defer func(target *os.File) {
		err := target.Close()
		if err != nil {
			log.Println(err)
		}
	}(target)

	// формируем block из []byte, номера и пока пустого хеша
	// каждый нарезанный блок отправляем в канал
	for {
		var block Block
		block.Body = make([]byte, BUFFER)
		n, err := target.Read(block.Body)
		if n == 0 || err == io.EOF {
			break
		}
		if n < len(block.Body) {
			block.Body = block.Body[:n]
		}
		block.Index = parts
		b <- block
		parts++
		block = Block{}
	}

	// а теперь ждём пока не придёт
	// количество подтверждений, равное
	// отправленным блокам
	for i := 0; i < parts; i++ {
		<-cmp
	}
	hasher := sha1.New()
	blocks := make(map[int][]byte)
	// собираем хеши пришедших обратно блоков
	// в словарь номер:хеш
	for i := 0; i < parts; i++ {
		temp := <-cs
		blocks[temp.Index] = temp.Hash
	}
	// и считаем единый sha из пришедших sha по порядку
	for i := 0; i < parts; i++ {
		hasher.Write(blocks[i])
	}
	sum = hasher.Sum(nil)
	return fmt.Sprintf("%x", sum)
}

// горутины считают sha отдельных блоков
func (rt *Routine) blockHasher(cmp chan bool, cs chan Block, b chan Block) {
	for {
		select {
		case <-rt.End:
			wg.Done()
			return
		case block := <-b:
			hasher := sha1.New()
			_, _ = hasher.Write(block.Body)
			block.Hash = hasher.Sum(nil)
			block.Body = nil
			// после подсчета отправляем обратно
			// блок с заполненным хешем и пустым body
			cs <- block
			// а в другой канал подтверждение, что закончили
			cmp <- true
			block = Block{}
		}
	}
}

func (rt *Routine) stop() {
	log.Printf("Routine [%s] is stopping", rt.Name)
	rt.End <- true
}

// help по -h
func showHelp() {
	fmt.Println(`
Usage:
hashbackup [-[a|s]dq | -h] source destination

Required args:
source
destination

Optional args:
a    Append mode
s    Synchronize mode
d    Delete old files
q    Quiet mode, no output
 `)
}

// Checkout разбор параметров и заполнение структур
func Checkout(args []string) (Options, Required, error) {
	var opt Options
	var req Required
	switch len(args) {
	case 0, 1:
		showHelp()
		os.Exit(0)
	case 2:
		if args[0][0] == '-' {
			if args[0][1] == 'h' {
				showHelp()
				os.Exit(0)
			} else {
				return opt, req, errors.New("wrong command line")
			}
		}
		opt.Append = true
		req.Source = args[0]
		req.Destination = args[1]
		return opt, req, nil
	case 3:
		for _, v := range args[0][1:] {
			switch v {
			case 'a':
				opt.Append = true
			case 's':
				opt.Sync = true
			case 'd':
				opt.Delete = true
			case 'q':
				opt.Quiet = true
			default:
				showHelp()
				os.Exit(0)
			}
		}
		req.Source = args[1]
		req.Destination = args[2]
		return opt, req, nil
	default:
		return opt, req, errors.New("wrong command line")
	}
	return Options{}, Required{}, nil
}

// NewsAndOrphans читаем каталог и находим новые файлы
func NewsAndOrphans(source string) (shaFile *os.File, sha file.ShaFile, news []string) {

	var files []string
	fileMap := make(map[string]string)

	// читаем каталог, получаем список файлов
	SrcDirEntry, err := os.ReadDir(source)
	checkFatal(err)
	for _, entry := range SrcDirEntry {
		files = append(files, entry.Name())
	}
	// убираем .sha из списка
	for k, v := range files {
		if v == ".sha1" {
			files = append(files[:k], files[k+1:]...)
			break
		}
	}

	sha.Path = source
	sha.Array = make(map[string]string)
	// читаем .sha, заполняем структуру
	shaFile, err = sha.ReadFile()
	checkFatal(err)
	// если в списке файлов есть файлы
	// которых нет в .sha - это новые файлы
	for _, v := range files {
		if _, ok := sha.Array[v]; !ok {
			news = append(news, v)
		} else {
			fileMap[v] = sha.Array[v]
		}
	}
	sha.Array = fileMap
	// возвращаем указатель на .sha,
	// структуру и список новых файлов
	return
}

// RewriteShaFile обнуляем и переписываем .sha
func RewriteShaFile(shaFile *os.File, shaMap map[string]string) {
	var shaNames []string
	err := shaFile.Truncate(0)
	check(err)
	_, err = shaFile.Seek(0, 0)
	check(err)
	for k := range shaMap {
		shaNames = append(shaNames, k)
	}
	sort.Strings(shaNames)
	for _, v := range shaNames {
		str := fmt.Sprintf("%s   %s\r\n", shaMap[v], v)
		_, err = shaFile.WriteString(str)
	}
}

type Print struct {
	Str   string
	Type  int
	Quiet bool
}

// горутина печати
// есть 4 типа начала строки
// если новая строка из канала по типу
// отличается от предыдущей - делаем отступ
func logPrint(p chan Print) {
	types := []string{"New file:", "Modified file:", "Missing from source:", "Copying file:"}
	line := 10 // out of array
	for {
		msg := <-p
		if line == msg.Type {
			if !msg.Quiet {
				fmt.Println(types[msg.Type], msg.Str)
			}
		} else {
			line = msg.Type
			if !msg.Quiet {
				fmt.Println("")
				fmt.Println(types[msg.Type], msg.Str)
			}
		}
	}
}

func main() {

	var fc FileCutter
	var pointers []*Routine

	completed := make(chan bool, 1e6)
	checksums := make(chan Block, 1e6)
	blocks := make(chan Block, 32)
	prints := make(chan Print, 1)

	//разбор параметров
	opt, req, err := Checkout(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}

	Q := opt.Quiet

	// srcShaFile - указатель на sha файл в source
	// srcSha - структура .sha c путём и списком хешей
	// srcNews - список новых файлов в source
	srcShaFile, srcSha, srcNews := NewsAndOrphans(req.Source)
	// dstShaFile - указатель на sha файл в destination
	// dstSha - структура .sha c путём и списком хешей
	// dstOld - список орфанов в destination
	dstShaFile, dstSha, dstOld := NewsAndOrphans(req.Destination)
	defer srcShaFile.Close()
	defer dstShaFile.Close()

	// синхронизация sha списков
	// если в destination есть то, чего нет в source - это орфан
	for k := range dstSha.Array {
		if _, ok := srcSha.Array[k]; !ok {
			dstOld = append(dstOld, k)
		}
	}

	// если в source есть то, чего нет в destination - это новый файл
	for k := range srcSha.Array {
		if _, ok := dstSha.Array[k]; !ok {
			srcNews = append(srcNews, k)
		}
	}

	go logPrint(prints)
	cpus := runtime.NumCPU()
	// одна горутина будет нарезать файл на части
	fc = FileCutter{Routine: Routine{Name: "FileCutter", End: make(chan bool, 1)}}
	// numcpu горутин будут считать sha
	for i := 0; i < cpus; i++ {
		bh := BlockHasher{Routine: Routine{Name: "BlockHasher", End: make(chan bool, 1)}}
		pointers = append(pointers, &bh.Routine)
		go bh.blockHasher(completed, checksums, blocks)
		wg.Add(1)
	}
	defer func() {
		for _, pts := range pointers {
			pts.End <- true
		}
		wg.Wait()
	}()

	// если есть новые файлы
	// озвучиваем их, если мы не в quiet mode
	if len(srcNews) > 0 {
		for _, v := range srcNews {
			prints <- Print{Str: v, Type: 0, Quiet: Q}
		}
	}

	// если в -d режиме удаления орфанов
	// озвучиваем их, если мы не в quiet mode и удаляем
	if opt.Delete {
		if len(dstOld) > 0 {
			for _, v := range dstOld {
				prints <- Print{Str: v, Type: 2, Quiet: Q}
			}
			for _, v := range dstOld {
				delete(dstSha.Array, v)
				path := fmt.Sprintf(req.Destination + string(os.PathSeparator) + v)
				err := os.Remove(path)
				check(err)
			}
		}
	}

	// список для копирования
	// здесь будут и новые и измененные файлы
	forCopy := make(map[string]string)

	// если в -s режиме синхронизации
	// пересчитываем sha файлов,
	// которые есть и в source и в destination
	// измененные файлы идут в список для копирования
	if opt.Sync {
		for k := range srcSha.Array {
			if _, ok := dstSha.Array[k]; ok {
				path := fmt.Sprintf(req.Source + string(os.PathSeparator) + k)
				srcSha.Array[k] = fc.fileCutter(path, completed, checksums, blocks)
				if srcSha.Array[k] != dstSha.Array[k] {
					prints <- Print{Str: k, Type: 1, Quiet: Q}
					forCopy[k] = srcSha.Array[k]
				}
			}
		}
	}

	// новые файлы идут в список для копирования с пустым sha
	for _, v := range srcNews {
		forCopy[v] = ""
	}

	// если есть что копировать из source в destination
	// заполняем пустые sha в списке для копирования
	if len(forCopy) > 0 {
		for k := range forCopy {
			if forCopy[k] == "" {
				path := fmt.Sprintf(req.Source + string(os.PathSeparator) + k)
				srcSha.Array[k] = fc.fileCutter(path, completed, checksums, blocks)
				forCopy[k] = srcSha.Array[k]
			}
		}
		// и копируем
		for k := range forCopy {
			srcPath := fmt.Sprintf(req.Source + string(os.PathSeparator) + k)
			dstPath := fmt.Sprintf(req.Destination + string(os.PathSeparator) + k)
			prints <- Print{Str: k, Type: 3, Quiet: Q}
			source, err := os.Open(srcPath)
			checkFatal(err)
			destination, err := os.Create(dstPath)
			checkFatal(err)
			buffer := make([]byte, BUFFER)
			for {
				n, err := source.Read(buffer)
				if err != nil && err != io.EOF {
					log.Fatal(err)
				}
				if n == 0 {
					break
				}
				_, err = destination.Write(buffer[:n])
				checkFatal(err)
				buffer = buffer[:0]
			}
			dstSha.Array[k] = forCopy[k]
			source.Close()
			destination.Close()
		}
	}
	// перезаполняем .sha в source и destination
	RewriteShaFile(srcShaFile, srcSha.Array)
	RewriteShaFile(dstShaFile, dstSha.Array)
}
