package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/chzyer/readline"
)

func main() {
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "MySQL [(none)]> ",          // 主提示符
		HistoryFile:     "/tmp/readline_history.tmp", // 历史记录文件
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		panic(err)
	}
	defer rl.Close()

	fmt.Println("Welcome to the MySQL client. Type 'exit' to quit.")

	var inputBuffer []string // 用于存储多行输入

	for {
		// 根据是否已有输入动态调整提示符
		if len(inputBuffer) > 0 {
			rl.SetPrompt("    -> ") // 多行输入提示符
		} else {
			rl.SetPrompt("MySQL [(none)]> ") // 主提示符
		}

		line, err := rl.Readline()
		if err != nil {
			if err == io.EOF { // 处理 Ctrl+D
				break
			}
			if err == readline.ErrInterrupt { // 处理 Ctrl+C
				if len(inputBuffer) == 0 {
					break
				} else {
					// 清空缓冲区，重新输入
					inputBuffer = nil
					fmt.Println("Input cleared. Starting over.")
					continue
				}
			}
			fmt.Fprintln(os.Stderr, "Error:", err)
			continue
		}

		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		// 如果输入 exit 或 quit，退出程序
		if line == "exit" || line == "quit" {
			fmt.Println("Bye!")
			break
		}

		// 将当前行加入缓冲区
		inputBuffer = append(inputBuffer, line)

		// 如果当前行以分号结束，表示输入完成
		if strings.HasSuffix(line, ";") {
			// 组合完整的输入文本
			fullInput := strings.Join(inputBuffer, " ")
			fmt.Println("Executing SQL:")
			fmt.Println(fullInput)

			// 将完整输入写入历史记录
			if err := rl.SaveHistory(fullInput); err != nil {
				fmt.Fprintln(os.Stderr, "Error saving history:", err)
			}

			// 清空缓冲区，准备接受下一条命令
			inputBuffer = nil
		}
	}
}
