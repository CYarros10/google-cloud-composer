#!/usr/bin/perl 
foreach(<>) {  
chomp;  
($store,$sale) = (split(/t/,$_))[2,4];  
print "$storet$salen";  
#print "{0}t{1}".format($store,$sale); 
}