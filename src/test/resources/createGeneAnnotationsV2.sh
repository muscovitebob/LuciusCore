#!/bin/bash
awk -v firstColumn=2 -v secondColumn=7 -v firstValue="LM" -v secondValue="---" '
	BEGIN {
		FS = OFS = "\t";
	}
	NR == 1 {
		$0 = "probesetID\tdataType\tSYMBOL\tENTREZID\tENSEMBL\tGENENAME\tGENEFAMILY"
		print $0
	}
	NR > 1 {
        for ( i = NF - 1; i > firstColumn; i-- ) {
            	$i = $(i-1);
        }
	for ( j = NF - 1; j > secondColumn; j-- ) {
		$j = $(j-1);
	}
        $i = firstValue;
	$j = secondValue
        print $0;
	}
' geneAnnotations.txt > geneAnnotationsV2.txt
