#!/usr/bin/env groovy

def inFile = "."
def outFile = "."
def files = new File("${this.args[0]}").listFiles()
def intermFiles = []
def inFiles = []
def loaderFiles = []

files.each {  file ->
    def name = file.name

    if (name.endsWith('interm_in.csv')) {
        intermFiles << file
    }   else if (name.endsWith('_loader.csv')) {
        loaderFiles << file
    }   else if (name.endsWith('_in.csv')){
        inFiles << file
    }
}

def numFile =0
inFiles.each { inF ->
    def fileName = inF.name
     println "Elaborating file: ${fileName}"
    def provider = getProviderFromFileName(fileName)
    def flowType = getFLowTypeFromFileName(fileName)
    InputStream inputStream = inF.newInputStream()
    Reader reader = inputStream.newReader('UTF-8')
    if (reader.ready()) reader.readLine()
    File output = new  File("${outFile}/out-in-${numFile}.csv")
    if (output.exists()) output.delete()
    try {
        while(reader.ready()) {
            def line = reader.readLine()
            def tokens = getTokensFromLine(line)
            def pubDocId = tokens[0].toUpperCase()
            def batchId = tokens[1].toUpperCase()
            def timestamp = tokens[2].toUpperCase()
            def deliveryInfo = getDeliveryInfoFromLine(line)
            def year =  getYearFromLine(line)
            def month = getMonthFromLine(line)
            // (provider string, flowTYpe string,publisher_doc_id string, batch_id string, status string, tstamp string, delivery string , year string, month string,XP string)
            def newLine = "${provider};${flowType};${pubDocId};${batchId};IN;${timestamp};${deliveryInfo};${year};${month};null\n"
            output.append(newLine)

        }
    }   catch(e) {

    } finally {
        reader.close()
    }
    numFile++
}

numFile = 0
intermFiles.each {  intermF ->
    def fileName = intermF.name
    println "Elaborating file: ${fileName}"
    def provider = getProviderFromFileName(fileName)
    def flowType = getFLowTypeFromFileName(fileName)
    InputStream inputStream = intermF.newInputStream()
    Reader reader = inputStream.newReader('UTF-8')
    if (reader.ready()) reader.readLine()
    File output = new  File("${outFile}/out-interm-${numFile}.csv")
    if (output.exists()) output.delete()
    try {
        while(reader.ready()) {
            def line = reader.readLine()
            def tokens = getTokensFromLine(line)
            def pubDocId = tokens[0]?.toUpperCase()
            def batchId = tokens[1]?.toUpperCase()
            def status = tokens[2]?.toUpperCase()
            def timestamp = tokens[3]?.toUpperCase()
            def deliveryInfo = getDeliveryInfoFromLine(line)
            def year =  getYearFromLine(line)
            def month = getMonthFromLine(line)
            // (provider string, flowTYpe string,publisher_doc_id string, batch_id string, status string, tstamp string, delivery string , year string, month string,XP string)
            def newLine = "${provider};${flowType};${pubDocId};${batchId==null?"NULL":batchId};${status};${timestamp};${deliveryInfo};${year};${month};null\n"
            output.append(newLine)

        }
    }   catch(e) {

    } finally {
        reader.close()
    }
    numFile++

}

numFile = 0
loaderFiles?.each {  loaderf ->
    def fileName = loaderf.name
    def tks = fileName.split("_")
    def provider = tks[0].tokenize(".")[0]
    def flowType = tks[0].tokenize(".")[1]
    println "Elaborating file: ${loaderf}"
    InputStream inputStream = loaderf.newInputStream()
    Reader reader = inputStream.newReader('UTF-8')
    if (reader.ready()) reader.readLine()
    File output = new  File("${outFile}/out-loader-${numFile}.csv")
    if (output.exists()) output.delete()
    try {
        while(reader.ready()) {
            def line = reader.readLine()
            def tokens = getTokensFromLine(line)
            def pubDocId = tokens[0]
            def XP = tokens[1]
            def batchId = tokens[2]
            def timeStamp = tokens[3]
            def deliveryInfo = getDeliveryInfoFromLine(line)
            def year =  getYearFromLine(line)
            def month = getMonthFromLine(line)
            // (provider string, flowTYpe string,publisher_doc_id string, batch_id string, status string, tstamp string, delivery string , year string, month string,XP string)
            def newLine = "${provider};${flowType};${pubDocId};${batchId};PRODUCED;${timeStamp};${deliveryInfo};${year};${month};${XP}\n"
            output.append(newLine)

        }
    }   catch(e) {

    } finally {
        reader.close()
    }
    numFile++

}

def getTokensFromFileName(fileName) {
    fileName.split("_")
}

def getProviderFromFileName(fileName) {
    def tks = getTokensFromFileName(fileName)
    tks[2].tokenize(".")[0]
}

def getFLowTypeFromFileName(fileName) {
    def tks = getTokensFromFileName(fileName)
    tks[2].tokenize(".")[1]
}

def getTokensFromLine(line) {
    def tokens = line.split(';')
    def upTokens = []
    tokens.each {
        upTokens << it?.toUpperCase()
    }
    return upTokens
}

def getDeliveryInfoFromLine(line) {
    tokens = getTokensFromLine(line)
    def deliveryInfo = tokens[0].tokenize('_')[0]
}

def getYearFromLine(line) {
    tokens = getTokensFromLine(line)
    tokens[-1].tokenize('_')[0][0..3]
}

def getMonthFromLine(line) {
    tokens = getTokensFromLine(line)
    tokens[-1].tokenize('_')[0][4..5]
}