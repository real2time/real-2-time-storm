#! /usr/bin/python

import SocketServer
import json
import fileinput
import subprocess
import os

STORM_HOME = os.getenv('STORM_HOME', '/opt/apache-storm-0.9.3')
NIMBUS_HOST = os.getenv('NIMBUS_HOST', '127.0.0.1')

R2T_STORM_HOME = os.getenv('R2T_STORM_HOME', '/opt/real-2-time-storm')
R2T_STORM_LISTEN_ADDRESS = os.getenv('R2T_STORM_LISTEN_ADDRESS', '127.0.0.1')
R2T_STORM_LISTEN_PORT = os.getenv('R2T_STORM_LISTEN_PORT', 44420)

PATH_MVN_STORM_COMPILE = R2T_STORM_HOME
PATH_TEMPLATE_FILE =  R2T_STORM_HOME + "/src/main/topologies/Real2TimeTemplate.java"
PATH_TO_TOPOLOGY_SOURCE_CODE = R2T_STORM_HOME + "/src/main/topologies"
PATH_JAR_STORM= R2T_STORM_HOME + "/target/real2time-storm-0.0.1-jar-with-dependencies.jar"

PATH_STORM_BIN = STORM_HOME + "/bin/storm"

class MyTCPServer(SocketServer.ThreadingTCPServer):
    allow_reuse_address = True

class MyTCPServerHandler(SocketServer.BaseRequestHandler):

    def returnRequest(self,data):
        return  """HTTP/1.1 200 OK
                  Content-Type: text/json;charset=utf-8

                """ + json.dumps(data)

    def handle(self):
        try:
            peticion = self.request.recv(10000).strip()
            ini = peticion.find("{")
            print peticion
            jsonTmp = peticion[ini:]
            print jsonTmp
            data = json.loads(jsonTmp)
            #self.run(data)
            #Se parsea el json y se envia el codigo java al fichero
            self.parseJson(data);
            print "-------------------Compiling----------------------"
            returnCodeCompile = self.compileTopology()
            if (returnCodeCompile == 0):
                self.request.sendall(self.returnRequest({'compile':'true'}))

                print "-------------------Send to cluster----------------------"
                returnCodeRunTopology = self.runTopology(data["_id"],data["_id"])
                if (returnCodeRunTopology == 0):
                    self.request.sendall(self.returnRequest({'upload':'true'}))
                else:
                    self.request.sendall(self.returnRequest({'upload':'false'}))
                print "-------------------End----------------------"
                print ""
            else:
                self.request.sendall(self.returnRequest({'compile':'false'}))

            # send some 'ok' back
            # self.request.sendall(json.dumps({'return':'ok'}))
        except Exception, e:
            print "Exception wile receiving message: ", e

    def values(self,value):

        config = ""
        for a in value:
            config = config + "\"" + str(value[a]) + "\","

        return config[:-1]

    def writeFile(self,idFile,string):

        f = open(PATH_TEMPLATE_FILE,'r')
        filedata = f.read()
        f.close()

        newdata = filedata.replace("//[replace_here]",string)
        newdata2 = newdata.replace("Real2TimeTemplate",idFile)

        f = open(PATH_TO_TOPOLOGY_SOURCE_CODE+"/"+str(idFile)+".java",'w')
        f.write(newdata2)
        f.close()

    def set_default(self,obj):
        if isinstance(obj, set):
            return list(obj)
        raise TypeError

    def scapeSlash(self,json):
        return (str(json).replace('u\'',"\\\"").replace("'","\\\""))

    #Buscamos todos los campos necesarios de los bolts necesarios para un spout
    def searchFieldsSpouts(self,modules,json,idSpout):

        setRequiredFields = set()
        for wire in json["config"]["wires"]:
            if (idSpout ==  int(wire["src"]["moduleId"])):
                for field in modules[wire["tgt"]["moduleId"]][4]:
                    setRequiredFields.add(field)
        return setRequiredFields

    def searchSameSpoutForBolt(self,modules,json,idBolt):
        idSpouts= []
        i = 0
        for wire in json["config"]["wires"]:
            if (idBolt ==  int(wire["tgt"]["moduleId"])):
                i += 1
                idSpouts.append(wire["src"]["moduleId"])
        return (i,idSpouts)

    def makeMultipleBolt(self,modules,json,idBolt):
        typeGrouping= ".shuffleGrouping"
        group = ""
        (countSpouts,idSpout) = self.searchSameSpoutForBolt(modules,json,idBolt)
        jsonConfig = "\"[{\\\"requiredFields\\\":"+self.scapeSlash(modules[idBolt][4])+","+self.scapeSlash(modules[idBolt][1])[1:]+"]\""
        builder = "builder.setBolt(\"{0}_{1}\", new {2}({3}),1)".format(json["_id"],idBolt,modules[idBolt][0],jsonConfig)
        for i in range(0,countSpouts):
            group= group + typeGrouping+"(\"{0}_{1}\")".format(json["_id"],idSpout[i])
        return builder + group +";"

    def parseJson(self,jsonRecive):
        codeJava = ""
        modules = []
        spouts = []
        bolts = []

        for module in jsonRecive["config"]["modules"]:
            modules.append((module["name"],module["value"],module["storm_type"],module["emitedFields"],module["requiredFields"]))

        for idSpout in range(0,len(modules)):
            if (modules[idSpout][2] == "spout"):
                if (modules[idSpout][0] == "kafka"):
                    fields = self.searchFieldsSpouts(modules,jsonRecive,idSpout)
                    jsonConfig = "\"[{\\\"emitedFields\\\":"+ (json.dumps(fields, default=self.set_default)).replace("\"","\\\"") +","+self.scapeSlash(modules[idSpout][1])[1:]+"]\""
                    spouts.append("BrokerHosts hosts = new ZkHosts(\"{2}\");String topicName = \"{3}\"; SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, \"/\" + topicName, UUID.randomUUID().toString());spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);builder.setSpout(\"{0}_{1}\", kafkaSpout, 1);".format(jsonRecive["_id"],idSpout,modules[idSpout][1]["ZkHosts"],modules[idSpout][1]["topicName"]))
                else:
                    fields = self.searchFieldsSpouts(modules,jsonRecive,idSpout)
                    jsonConfig = "\"[{\\\"emitedFields\\\":"+ (json.dumps(fields, default=self.set_default)).replace("\"","\\\"") +","+self.scapeSlash(modules[idSpout][1])[1:]+"]\""
                    spouts.append("builder.setSpout(\"{0}_{1}\", new {2}({3}),1);".format(jsonRecive["_id"],idSpout,modules[idSpout][0], jsonConfig))

        for wire in jsonRecive["config"]["wires"]:
            #print modules[wire["src"]["moduleId"]][0]+" -> "+modules[wire["tgt"]["moduleId"]][0]
            #if target is a Bolt
            if (modules[wire["tgt"]["moduleId"]][2] == "bolt"):
                idBolt = wire["tgt"]["moduleId"]
                bolts.append(self.makeMultipleBolt(modules,jsonRecive,idBolt))

        bolts = set(bolts)
        for spout in spouts:
            codeJava = codeJava + spout
        for bolt in bolts:
            codeJava = codeJava + bolt

        self.writeFile("run_"+jsonRecive["_id"],codeJava)

    def compileTopology(self):
        bashCommand = "mvn clean package -DskipTests=true -f {0}/pom.xml".format(PATH_MVN_STORM_COMPILE)
        process = subprocess.Popen(bashCommand.split())
        #process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output = process.communicate()[0]
        return process.returncode

    def runTopology(self,topologyClass,topologyName):

        bashCommand = "{0} jar -c nimbus.host={1} {2} topologies.run_{3} {4}".format(PATH_STORM_BIN, NIMBUS_HOST, PATH_JAR_STORM, str(topologyClass), topologyName)
        print bashCommand
        process = subprocess.Popen(bashCommand.split())
        #process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output = process.communicate()[0]
        return process.returncode


print "Starting server on " + R2T_STORM_LISTEN_ADDRESS + ":" + str(R2T_STORM_LISTEN_PORT) + " ..."

server = MyTCPServer((R2T_STORM_LISTEN_ADDRESS, int(R2T_STORM_LISTEN_PORT)), MyTCPServerHandler)
server.serve_forever()
