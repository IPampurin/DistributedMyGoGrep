package master

import (
	"fmt"

	"github.com/IPampurin/DistributedMyGoGrep/pkg/configuration"
	"github.com/IPampurin/DistributedMyGoGrep/pkg/models"
)

// printResults выводит итоговые результаты в stdout
func printResults(cfg *configuration.Config, results []*models.Result) error {

	if cfg.Count {

		total := 0
		for _, res := range results {

			if res == nil {
				return fmt.Errorf("шард не имеет результата")
			}

			total += res.Count
		}

		fmt.Printf("%d\n", total)

	} else {

		for _, res := range results {

			if res == nil {
				return fmt.Errorf("шард не имеет результата")
			}

			for _, line := range res.Lines {
				fmt.Println(line)
			}
		}
	}

	return nil
}
