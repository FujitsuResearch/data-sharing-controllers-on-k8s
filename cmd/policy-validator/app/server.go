// Copyright (c) 2022 Fujitsu Limited

package app

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/cmd/policy-validator/app/options"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/controller/validation"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

func run(policyOptions *options.PolicyValidatorOptions) error {
	kubernetesConfig, err := clientcmd.BuildConfigFromFlags(
		policyOptions.KubernetesConfig.ApiServerUrl,
		policyOptions.KubernetesConfig.KubeConfig)
	if err != nil {
		return fmt.Errorf("Error building kubeconfig: %v", err.Error())
	}

	kubeClient, err := kubernetes.NewForConfig(kubernetesConfig)
	if err != nil {
		return fmt.Errorf(
			"Error building kubernetes clientset: %v", err.Error())
	}

	policyValidator, err := validation.NewPolicyValidator(
		policyOptions.DataStore, kubeClient)
	if err != nil {
		return err
	}

	return policyValidator.Run(
		policyOptions.Endpoint, policyOptions.Path, policyOptions.TlsConfig)
}

func NewPolicyValidatorCommand() *cobra.Command {
	opts := options.NewPolicyValidatorOptions()

	cmd := &cobra.Command{
		Use: "shadowy-policy-validator",
		Long: `the shadowy polcy validator checks creations and updates for ` +
			`the lfetime policies`,
		Run: func(cmd *cobra.Command, args []string) {
			util.PrintFlags(cmd.Flags())

			if err := run(opts); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err.Error())
				os.Exit(1)
			}
		},
	}

	opts.AddFlags(cmd.Flags())

	return cmd
}
