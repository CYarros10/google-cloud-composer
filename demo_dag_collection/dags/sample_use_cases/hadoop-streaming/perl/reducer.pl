#!/usr/bin/perl 
use List::Util qw(sum); 
my %hashTab; 
$totalsale = 0; 
foreach (<>) {  
chomp;  
@data = split(/t/,$_); 
if($#data == 1) {  
($store,$sale)=@data; 
 if(exists($hashTab{$store}))  {  $hashTab{$store} = sum($hashTab{$store} + $sale);  
}  else  {  
$hashTab{$store} = $sale;  
}  
# $hashTab{$store} = $totalsale; 
} 
} 
foreach (keys(%hashTab))  {  print "$_ $hashTab{$_}n";  
}