
/**
 * This groovy script will aggregate data that are analyzed by
 * hive, putting them in one nice line
 */

String actualId = null
Map values = resetMap()

private Map resetMap() {
    return ['DELIVERY_ID':'UNKNOWN', 'PROVIDER':"UNKNOWN", 'FLOWTYPE':"UNKNOWN",
            'DELIVERY':"UNKNOWN",'IN': 0, 'IGNORED': 0, 'BUSINNESREJECT': 0,
            'UNKNOWNERROR': 0, 'NOPDF': 0, 'UNKNOWNCONTENTTYPE':0, 'OK': 0, 'PRODUCED': 0, 'FIRST_XP': "NOT ASSIGNED",
            'LAST_XP': "NOT ASSIGNED"]
}

private void output(Map values) {
    if (values != resetMap())
        println values.collect{key, value -> "${value}"}.join("\t")
}

private def fillMap(List<String> tokens, values) {
    values.'DELIVERY_ID' = tokens[0]
    values.'PROVIDER' = tokens[1]
    values.'FLOWTYPE' = tokens[2]
    values.'DELIVERY' = tokens[3]
    values."${tokens[4]}" = tokens[5]
    if (tokens[6] != "null" && tokens[7] != "null") {
        values.'FIRST_XP' = tokens[6]
        values.'LAST_XP' = tokens[7]
    }
}

System.in.eachLine() { line ->
    def tokens = line.tokenize("\t")
    def id = "${tokens[0]}_${tokens[1]}_${tokens[2]}"
    if (actualId && actualId == id) {
        fillMap(tokens, values)

    } else {
        output(values)
        actualId = id
        values=resetMap()
        fillMap(tokens, values)
    }
}
output(values)


