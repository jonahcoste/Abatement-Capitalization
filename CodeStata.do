clear *
set maxvar 50000
use "/Volumes/ExternalHD2/PhilCap/V6/42.dta", clear


gen dt = date(date, "YMD")
sort importparcelid dt
by importparcelid: gen salenum = _n
xtset importparcelid salenum
*Drop duplicate sales
gen days = dt - L.dt
gen delta = log(salespriceamount) - log(L.salespriceamount)
drop if days<=365 & delta==0
*Drop properties sold more once within a year
by importparcelid: egen mindays = min(days)
keep if mindays >= 365
drop salenum mindays delta



sort importparcelid dt
by importparcelid: gen salenum = _n
*drop if only one sale
by importparcelid: gen saleN = _N
drop if saleN == 1

xtset importparcelid salenum
gen l_salespriceamount = L.salespriceamount
gen l_year=L.year
gen l_age=L.age
gen time = year-l_year
gen lnpricedelta = log(salespriceamount) - log(l_salespriceamount)

*drop transaction pairs remodeled. between sales
drop if !missing(yearremodeled) & yearremodeled <= year & yearremodeled >= l_year

* drop tranactions more than 100 log points different than county trend
forvalues i= 1994/2020 { 
    gen dyear`i' = 0
	replace dyear`i' = dyear`i' + 1 if year == `i'
	replace dyear`i' = dyear`i' - 1 if l_year == `i'
}
reg lnpricedelta i.fips#c.dyear* if (lnpricedelta>-2 & lnpricedelta<2), nocons
predict resid, r
sort importparcelid
by importparcelid: egen maxresid = max(resid)
by importparcelid: egen minresid = min(resid)
keep if minresid>-1 & maxresid<1

bysort propertyzip: gen zipN = _N
keep if zipN>1000

gen abat = 0
replace abat = 1 if fips==42101 & yearb>=2000

drop salenum saleN
sort importparcelid dt
by importparcelid: gen saleN = _N
drop if saleN == 1


*Price change distibution
kdensity lnpricedelta if abat==0, addplot(kdensity lnpricedelta if abat==1)
*Property Statistics
by importparcelid: gen propn = _n
tab abat if propn==1 & fips==42101
tab abat if propn==1 & fips!=42101
gen typsfr = propertylandusestndcode=="RR999" | propertylandusestndcode=="RR101"
gen typrow = propertylandusestndcode=="RR104" | propertylandusestndcode=="RR105" | propertylandusestndcode=="RR108"
gen typcnd = propertylandusestndcode=="RR106"
sum typsfr typrow typcnd if propn==1 & abat==1 & fips==42101
sum typsfr typrow typcnd if propn==1 & abat==0 & fips==42101
sum typsfr typrow typcnd if propn==1 & abat==0 & fips!=42101
sum typsfr typrow typcnd if propn==1 & abat==1 & age<=10
sum typsfr typrow typcnd if propn==1 & abat==0 & age<=10
bysort propertyzip: gen zipn = _n
tab fips if zipn==1

*Repeat Transaction Statistics
tab abat if !missing(lnpricedelta) & fips==42101
tab abat if !missing(lnpricedelta) & fips!=42101
gen yeardif  = year-l_year
sum yeardif lnpricedelta if !missing(lnpricedelta) & abat==1 & fips==42101
sum yeardif lnpricedelta if !missing(lnpricedelta) & abat==0 & fips==42101
sum yeardif lnpricedelta if !missing(lnpricedelta) & abat==0 & fips!=42101
sum yeardif lnpricedelta if !missing(lnpricedelta) & abat==1 & (age<=10 | l_age<=10)
sum yeardif lnpricedelta if !missing(lnpricedelta) & abat==0 & (age<=10 | l_age<=10)
*Transaction Statistics
gen leq10 = age<=10
tab abat leq10 if fips==42101
tab abat leq10 if fips!=42101
tab abat leq10
sum salespriceamount if abat==1 & fips==42101
sum salespriceamount if abat==0 & fips==42101
sum salespriceamount if abat==0 & fips!=42101
sum salespriceamount if abat==1 & age<=10
sum salespriceamount if abat==0 & age<=10

gen lns = log(salespriceamount)

drop propn zipn typsfr typrow typcnd yeardif leq10


gen abat_int = 0
replace abat_int = abat_int + 8.752529104 if age==0
replace abat_int = abat_int + 8.252529104 if age==1
replace abat_int = abat_int + 7.267529104 if age==2
replace abat_int = abat_int + 6.312079104 if age==3
replace abat_int = abat_int + 5.385292604 if age==4
replace abat_int = abat_int + 4.486309699 if age==5
replace abat_int = abat_int + 3.614296281 if age==6
replace abat_int = abat_int + 2.768443265 if age==7
replace abat_int = abat_int + 1.94796584 if age==8
replace abat_int = abat_int + 1.152102738 if age==9
replace abat_int = abat_int + 0.380115529 if age==10

replace abat_int = 0 if abat==0


gen abat_ind = 0
replace abat_ind=-1 if age>=11 & l_age<=10 & abat==1


gen typrow = propertylandusestndcode=="RR104" | propertylandusestndcode=="RR105" | propertylandusestndcode=="RR108"
gen typcnd = propertylandusestndcode=="RR106"

*PHL only
gen gage = age<=10
gen abatgage = abat*gage

reghdfe lns gage abatgage if fips==42101, absorb(i.importparcelid i.year#i.propertyzip) noconstant vce(r)

estimates store phl

gen zeroage = 0
replace zeroage = age+1 if age<=10
*Column1/2
reghdfe lns 0.age#1.abat 1.age#1.abat 2.age#1.abat 3.age#1.abat 4.age#1.abat 5.age#1.abat 6.age#1.abat 7.age#1.abat 8.age#1.abat 9.age#1.abat 10.age#1.abat , absorb(i.zeroage i.importparcelid i.year#i.propertyzip) noconstant vce(r)

estimates store col12

lincom 0.age#1.abat/0.099080424

reghdfe lns gage abatgage , absorb(i.zeroage i.importparcelid i.year#i.propertyzip) noconstant vce(r)

estimates store col12g

*Column2/3
reghdfe lns 0.age#1.abat 1.age#1.abat 2.age#1.abat 3.age#1.abat 4.age#1.abat 5.age#1.abat 6.age#1.abat 7.age#1.abat 8.age#1.abat 9.age#1.abat 10.age#1.abat , absorb(i.age i.importparcelid i.year#i.propertyzip) noconstant vce(r)

estimates store col23

lincom 0.age#1.abat/0.099080424

reghdfe lns gage abatgage , absorb(i.age i.importparcelid i.year#i.propertyzip) noconstant vce(r)


estimates store col23g

*Column3/4
reghdfe lns 0.age#1.abat 1.age#1.abat 2.age#1.abat 3.age#1.abat 4.age#1.abat 5.age#1.abat 6.age#1.abat 7.age#1.abat 8.age#1.abat 9.age#1.abat 10.age#1.abat , absorb(i.age i.age#i.typcnd i.age#i.typrow i.importparcelid i.year#i.propertyzip) noconstant vce(r)

estimates store col34

lincom 0.age#1.abat/0.104155096

reghdfe lns gage abatgage , absorb(i.age i.age#i.typcnd i.age#i.typrow i.importparcelid i.year#i.propertyzip) noconstant vce(r)

estimates store col34g

*Column4/5
reghdfe lns 0.age#1.abat 1.age#1.abat 2.age#1.abat 3.age#1.abat 4.age#1.abat 5.age#1.abat 6.age#1.abat 7.age#1.abat 8.age#1.abat 9.age#1.abat 10.age#1.abat 11.age#1.abat 13.age#1.abat 14.age#1.abat 15.age#1.abat 16.age#1.abat 17.age#1.abat 18.age#1.abat 19.age#1.abat 20.age#1.abat, absorb(i.age i.age#i.typcnd i.age#i.typrow i.importparcelid i.year#i.propertyzip) noconstant vce(r)

test 13.age#1.abat 14.age#1.abat 15.age#1.abat 16.age#1.abat 17.age#1.abat 18.age#1.abat 19.age#1.abat

test 11.age#1.abat 13.age#1.abat 14.age#1.abat 15.age#1.abat 16.age#1.abat 17.age#1.abat 18.age#1.abat 19.age#1.abat

estimates store col45

lincom 0.age#1.abat/0.099080424

reghdfe lns gage abatgage 11.age#1.abat 13.age#1.abat 14.age#1.abat 15.age#1.abat 16.age#1.abat 17.age#1.abat 18.age#1.abat 19.age#1.abat 20.age#1.abat, absorb(i.age i.age#i.typcnd i.age#i.typrow i.importparcelid i.year#i.propertyzip) noconstant vce(r)

estimates store col45g

*Column5/6
reghdfe lns 0.age#1.abat 1.age#1.abat 2.age#1.abat 3.age#1.abat 4.age#1.abat 5.age#1.abat 6.age#1.abat 7.age#1.abat 8.age#1.abat 9.age#1.abat 10.age#1.abat 11.age#1.abat, absorb(i.age i.age#i.typcnd i.age#i.typrow i.importparcelid i.year#i.propertyzip) noconstant vce(r)

estimates store col56

lincom 0.age#1.abat/0.099080424

reghdfe lns gage abatgage 11.age#1.abat, absorb(i.age i.age#i.typcnd i.age#i.typrow i.importparcelid i.year#i.propertyzip) noconstant vce(r)

estimates store col556g

*abat intensity

reghdfe lns abat_int, absorb(i.age i.age#i.typcnd i.age#i.typrow i.importparcelid i.year#i.propertyzip) noconstant vce(r)

estimates store abatint

lincom abat_int/0.011829752

*Repeat Sales
forvalues j= 0/10 { 
    gen dage`j' = 0
	replace dage`j' = dage`j' + 1 if age == `j'
	replace dage`j' = dage`j' - 1 if l_age == `j'
	gen abatage`j' = dage`j'*abat
}

forvalues j= 11/11 { 
    gen d2age`j' = 0
	replace d2age`j' = d2age`j' + 1 if age == `j'
	replace d2age`j' = d2age`j' - 1 if l_age == `j'
	gen abat2age`j' = d2age`j'*abat
}
forvalues j= 12/119 { 
    gen d2age`j' = 0
	replace d2age`j' = d2age`j' + 1 if age == `j'
	replace d2age`j' = d2age`j' - 1 if l_age == `j'
}


reg lnpricedelta c.dyear*#i.propertyzip c.typ*#c.dage* c.typ*#c.d2age* dage* d2age* abatage* abat2age*, nocons r

estimates store colrs

lincom abatage0/0.099080424

*Pre-2005
reghdfe lns 0.age#1.abat 1.age#1.abat 2.age#1.abat 3.age#1.abat 4.age#1.abat 5.age#1.abat 6.age#1.abat 7.age#1.abat 8.age#1.abat 9.age#1.abat 10.age#1.abat 11.age#1.abat if (abat==0 | yearb<2005), absorb(i.age i.age#i.typcnd i.age#i.typrow i.importparcelid i.year#i.propertyzip) noconstant vce(r)

estimates store colearly

lincom 0.age#1.abat/0.099080424


*hedonic
gen lot = lotsize
replace lot = buildingareasqft if (buildingareasqft>lotsize | missing(lotsize)) & !missing(buildingareasqft)
gen lnlot = log(lot)
gen lnsqft = log(buildingareasqft)
gen hometype = .

replace hometype = 1
replace hometype = 2 if typcnd==1
replace hometype = 3 if typrow==1

gen baths = fullbath
replace baths = totalcalculatedbathcount if totalcalculatedbathcount>fullbath | missing(fullbath)
replace baths = totalactualbathcount if totalactualbathcount>baths & !missing(totalactualbathcount) | missing(baths)

reghdfe lns lnlot  lnsqft totalbedrooms baths abat 0.age#1.abat 1.age#1.abat 2.age#1.abat 3.age#1.abat 4.age#1.abat 5.age#1.abat 6.age#1.abat 7.age#1.abat 8.age#1.abat 9.age#1.abat 10.age#1.abat 11.age#1.abat, absorb(i.hometype#c.lnlot i.age#c.lnlot i.hometype#c.lnsqft i.age#c.lnsqft i.age i.age#i.typcnd i.age#i.typrow i.year#i.propertyzip) noconstant vce(r)

estimates store hed

lincom 0.age#1.abat/0.099080424

*propensity match
drop saleN
sort importparcelid dt
by importparcelid: gen saleN = _N
by importparcelid: gen salenum = _n
psmatch2 abat lns c.lns#i.hometype i.hometype if salenum==1, n(200)
bysort importparcelid: egen weight = max(_weight)

reghdfe lns 0.age#1.abat 1.age#1.abat 2.age#1.abat 3.age#1.abat 4.age#1.abat 5.age#1.abat 6.age#1.abat 7.age#1.abat 8.age#1.abat 9.age#1.abat 10.age#1.abat 11.age#1.abat [aw=weight], absorb(i.age i.importparcelid i.year#i.propertyzip) noconstant vce(r)

estimates store ps

lincom 0.age#1.abat/0.099080424
