// Copyright (c) 2022 Fujitsu Limited

package volumectl

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

type checksumOptions struct {
	filePath string
}

func newChecksumOptions() *checksumOptions {
	return &checksumOptions{}
}

func addChecksumFlags(cmd *cobra.Command, co *checksumOptions) {
	cmd.Flags().StringVar(
		&co.filePath, "file-path", co.filePath,
		"Path to a executable file to calculate the checksum")
	cmd.MarkFlagRequired("file-path")
}

func newChecksumCommand() *cobra.Command {
	opts := newChecksumOptions()

	subCmd := &cobra.Command{
		Use:  "calc-checksum",
		Long: `the shadowy volumectl calcuates a file checksum`,
		Run: func(cmd *cobra.Command, args []string) {
			util.PrintFlags(cmd.Flags())

			start := time.Now()

			checksum, err := util.CalculateChecksum(opts.filePath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err.Error())
				os.Exit(1)
			}

			elapsed := time.Since(start)

			fmt.Fprintf(
				os.Stdout, "[%v sec] Checksum for %s: %x\n",
				elapsed.Seconds(), opts.filePath, checksum)
		},
	}

	addChecksumFlags(subCmd, opts)

	return subCmd
}
