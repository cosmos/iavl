package iavl

import (
	"fmt"

	sha3 "github.com/miguelmota/go-solidity-sha3"
	"github.com/tendermint/tendermint/libs/common"
)

// This file implement fuzz testing by generating programs and then running
// them. If an error occurs, the Program that had the error is printed.

// A Program is a list of Instructions.
type Program struct {
	Instructions []instruction
}

func (p *Program) Execute(tree *MutableTree) (err error) {
	var errLine int

	defer func() {
		if r := recover(); r != nil {
			var str string

			for i, instr := range p.Instructions {
				prefix := "   "
				if i == errLine {
					prefix = ">> "
				}
				str += prefix + instr.String() + "\n"
			}
			err = fmt.Errorf("Program panicked with: %s\n%s", r, str)
		}
	}()

	for i, instr := range p.Instructions {
		errLine = i
		instr.Execute(tree)
	}
	return
}

func (p *Program) addInstruction(i instruction) {
	p.Instructions = append(p.Instructions, i)
}

func (p *Program) size() int {
	return len(p.Instructions)
}

type instruction struct {
	op      string
	k, v    []byte
	version int64
}

func (i instruction) Execute(tree *MutableTree) {
	switch i.op {
	case "SET":
		tree.Set(i.k, i.v)
	case "REMOVE":
		tree.Remove(i.k)
	case "SAVE":
		tree.SaveVersion()
	case "DELETE":
		tree.DeleteVersion(i.version)
	default:
		panic("Unrecognized op: " + i.op)
	}
}

func (i instruction) String() string {
	if i.version > 0 {
		return fmt.Sprintf("%-8s %-8s %-8s %-8d", i.op, i.k, i.v, i.version)
	}
	return fmt.Sprintf("%-8s %-8s %-8s", i.op, i.k, i.v)
}

// Generate a random Program of the given size.
func genRandomProgram(size int) *Program {
	p := &Program{}
	nextVersion := 1

	for p.size() < size {
		k, v := []byte(common.RandStr(1)), []byte(common.RandStr(1))

		switch common.RandInt() % 7 {
		case 0, 1, 2:
			p.addInstruction(instruction{op: "SET", k: k, v: v})
		case 3, 4:
			p.addInstruction(instruction{op: "REMOVE", k: k})
		case 5:
			p.addInstruction(instruction{op: "SAVE", version: int64(nextVersion)})
			nextVersion++
		case 6:
			if rv := common.RandInt() % nextVersion; rv < nextVersion && rv > 0 {
				p.addInstruction(instruction{op: "DELETE", version: int64(rv)})
			}
		}
	}
	return p
}

// loomchain based fuzz programs
func (p *Program) ExecuteBlock(tree *MutableTree) error {
	for _, instruction := range p.Instructions {
		instruction.Execute(tree)
	}
	return nil
}

func GenerateBlocks2(numBlocks, blockSize int) []*Program {
	fmt.Println()
	var history []*Program
	for i := 0; i < numBlocks; i++ {
		history = append(history, getRandomBlock2(blockSize))
	}
	return history
}

func GenerateBlocks(numBlocks, blockSize int) []*Program {
	fmt.Println()
	var history []*Program
	for i := 0; i < numBlocks; i++ {
		history = append(history, getRandomBlock(blockSize))
	}
	return history
}

func getRandomBlock(size int) *Program {
	p := &Program{}

	for p.size() < size {
		k, v := []byte(common.RandStr(1)), []byte(common.RandStr(1))

		switch common.RandInt() % 4 {
		case 0, 1, 2:
			p.addInstruction(instruction{op: "SET", k: k, v: v})
		case 3:
			p.addInstruction(instruction{op: "REMOVE", k: k})
		}
	}
	return p
}

func getRandomBlock2(size int) *Program {
	p := &Program{}

	for p.size() < size {
		r, v := []byte(common.RandStr(4)), []byte(common.RandStr(1))
		key := sha3.SoliditySHA3(r)
		switch common.RandInt() % 4 {
		case 0, 1, 2:
			p.addInstruction(instruction{op: "SET", k: key, v: v})
		case 3:
			p.addInstruction(instruction{op: "REMOVE", k: key})
		}
	}
	return p
}

func getRandomBlockWithRepeat(size int) *Program {
	p := &Program{}
	repeatKey := []byte("R")
	for p.size() < size {
		k, v := []byte(common.RandStr(1)), []byte(common.RandStr(1))

		switch common.RandInt() % 4 {
		case 0:
			p.addInstruction(instruction{op: "SET", k: k, v: v})
		case 3, 1:
			p.addInstruction(instruction{op: "SET", k: repeatKey, v: v})
		case 2:
			p.addInstruction(instruction{op: "REMOVE", k: k})
		}
	}
	return p
}
