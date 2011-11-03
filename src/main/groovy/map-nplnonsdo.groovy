#!/usr/bin/env groovy

/**
 * This groovy script will aggregate data that are analyzed by
 * hive, putting them in one nice line
 */

String actualId = null
Map values = resetMap()

private Map resetMap() {
    return ['PROVIDER':"UNKNOWN", 'FLOWTYPE':"UNKNOWN",
            'DELIVERY':"UNKNOWN",'IN': 0, 'IGNORED': 0, 'BUSINNESREJECT': 0,
            'UNKNOWNERROR': 0, 'NOPDF': 0, 'OK': 0, 'PRODUCED': 0, 'XPMIN': "NOT ASSIGNED",
            'XPMAX': "NOT ASSIGNED"]
}


System.in.eachLine() { line ->
    def tokens = line.tokenize("\t")
    def id = "${tokens[0]}_${tokens[1]}_${tokens[2]}"
    if (actualId && actualId == id) {
        fillMap(tokens, values)

    } else {
        toHive(values)
        actualId = id
        values=resetMap()
        fillMap(tokens, values)
    }
}


private def toHive(Map values) {
    if (values != resetMap())
        println values.collect{key, value -> "${key}: ${value}"}.join("\t")
}

private def fillMap(List<String> tokens, values) {
    values.'PROVIDER' = tokens[0]
    values.'FLOWTYPE' = tokens[1]
    values.'DELIVERY' = tokens[2]
    values."${tokens[3]}" = tokens[4]
    if (tokens[5] != "null" && tokens[6] != "null") {
        values.'XPMIN' = tokens[5]
        values.'XPMAX' = tokens[6]
    }
}