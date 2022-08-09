import os  

def extractParam(filename, paramTar):
    fileObj = open(filename)
    lastLine = fileObj.readlines()[-1]
    tokens = lastLine.split(',')
    for param in tokens:
        pair = param.split('=')
        if pair[0] == paramTar:
            return pair[1]
    return "error"

def main():
    targetParam = "tput"
    fileType = ["MAAT_","MVCC_"]
    direPath = "./"
    nums = ["0.0", "0.25", "0.5", "0.55", "0.6", "0.65", "0.7", "0.75", "0.8", "0.9"]
    results = []
    for cc in fileType:
        for i in nums:
            filename = direPath + "output_" + cc + i + ".txt"
            paramValue = extractParam(filename, targetParam)
            results+=[((filename, paramValue))]
    for item in results:
            print("filename:", item[0],targetParam+":", item[1])

main()
