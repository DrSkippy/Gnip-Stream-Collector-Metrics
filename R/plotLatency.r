#!/usr/bin/env Rscript
#
# Scott Hendirckson
#  shendrickson@gnip.com
#   2012-01-17
#
# Plot latency

library(ggplot2)
library(gridExtra)

dat<-read.table("./latency_data.csv", header=FALSE, sep=",")
colnames(dat) <- c("date","latency")
dat$dates <- as.POSIXlt(dat$date, "%Y-%M-%d %H:%M:%S")

dat$cat[0:length(dat$latency)] = "StockTwits"

summary(dat)


p0 <-qplot(dat$cat, dat$latency, data=dat, geom="boxplot"
	, main="Latency"
	, xlab=""
	, ylab="Latency (Sec)")

p1 <-qplot(as.factor(dat$dates$hour), dat$latency, data=dat, geom="boxplot"
	, main="Latency"
	, xlab="hour"
	, ylab="Latency (Sec)")

png(filename = "./latencyBoxPlots.png", width = 520, height = 700, units = 'px')
print(
   grid.arrange(p0, p1, ncol=1)
)
dev.off()
