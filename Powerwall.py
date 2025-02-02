#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import signal
import time
import os
import serial
import binascii
import enum
from datetime import datetime
import struct
import paho.mqtt.client as mqtt
import logging
import logging.handlers
import threading
import asyncio

from tesla_powerwall import Powerwall

logLevel = logging.INFO

GatewayIP = "192.168.1.63"
#GatewayIP = "192.168.1.59"
GatewayEmail = "j.e.hazan@gmail.com"
GatewayPassword = "KAAXD"

MQTT_USER = ""
MQTT_PASSWORD = ""
MQTT_SERVER = 'hamepi.local'

SLEEPTIME = 10

myDebug = False
repeatRun = False
pauseRun = False
droppedConnection = False

settables = {}


scriptPathAndName = None
lastModDate = None
def wait100ms():
    global scriptPathAndName,lastModDate
    if not scriptPathAndName:
        scriptPathAndName = os.path.realpath(__file__)
    if not lastModDate:
        lastModDate = time.ctime(os.path.getmtime(scriptPathAndName))
    if lastModDate != time.ctime(os.path.getmtime(scriptPathAndName)):
        print ('#### RELAUNCH ####')
        os.execv(sys.argv[0], sys.argv)
        sys.exit(0)
    time.sleep(0.1)

def connecthandler(mqc,userdata,flags,rc):
    global droppedConnection
    logging.info("Connected to MQTT broker with rc=%d" % (rc))
    if droppedConnection:
        logging.info("Dropped connection detected, setting Homie state to ready")
        client.publish(HomieRoot + "$state", "ready", 1, True)
        droppedConnection = False

def disconnecthandler(mqc,userdata,rc):
    global droppedConnection
    logging.error(f"Disconnected from MQTT broker with rc = {rc}")
    droppedConnection = True

def on_message_to_gateway(mosq, obj, msg):

    global SLEEPTIME
    global repeatRun
    global pauseRun

    msgValue = str(msg.payload.decode("utf-8"))
    logging.info("Command Topic: " + msg.topic + " Value: " + msgValue)
    t = msg.topic.split("/")
    node = t[2]
    prop = t[3]
    logging.info("Node: " + node + " Prop: " + prop)

    try:
        current_date = datetime.now()
        client.publish(SystemLastSeen, current_date.isoformat(), 1, True)

        if prop in settables:
            errorStr = ""
            cmd = settables[prop]
            if 'conv' in cmd:
                myConv = cmd['conv']
            else:
                myConv = ""

            # Special processing

            if myConv == 'Div10' or myConv == 'Div10Byte':
                if 'max' in cmd and 'min' in cmd:
                    val = int(float(msgValue) * 10)
                    if val > cmd['max'] or val < cmd['min']:
                        errorStr = "Value %d out of range for %s"% (val, prop)
                else:
                    errorStr = "Missing 'max' and 'min' attributes in readCmd for %s" % prop
            elif cmd['datatype'] == 'integer':
                val = int(msgValue)

            if cmd['datatype'] == 'enum':
                if 'format' in cmd:
                    modeToSet = -1

                    for num, modeName in cmd['format'].items():
                        if modeName == msgValue:
                            modeToSet = int(num)
                
                    if modeToSet == -1:
                        errorStr = "Mode %s not found for %s" % (modeName, prop)
                    val = modeToSet
                else:
                    errorStr = "Missing 'format' attribute in readCmd for %s" % prop
            elif cmd['datatype'] == 'boolean':
                if msgValue == 'true':
                    val = 1
                else:
                    val = 0
                


            if errorStr == "":
                if rmsg:
                    logging.info("Set %s to %s" % (prop, msgValue))
            else:
                logging.error(errorStr)
        elif prop == 'sleeptime':
            mySleeptime = int(msgValue)
            logging.info("Set SLEEPTIME to %d", mySleeptime)
            SLEEPTIME = mySleeptime
        elif prop == 'reread':
            repeatRun = False
            client.publish(SystemReread, "false", 1, True)
            logging.info("Read all values")
        elif prop == 'pause':
            if msgValue == 'true':
                pauseRun = True
                logging.info("Pausing")
            else:
                logging.info("Unpausing")
                pauseRun = False
        elif prop == 'relaunch':
            client.publish(SystemRelaunch, "false", 1, True)
            logging.info("RELAUNCH")
            os.execv(sys.argv[0], sys.argv)
            sys.exit(0)
            
    except:
        logging.error("Tesla Gateway Error: " + str(sys.exc_info()[1]))
              

async def publishPowerwallValues():
    global commStarted
    global repeatRun

    if commStarted:

        for cmd in readCmds:
            if pauseRun:
                logging.info("Stopped publishing values because of pause")
                return

            if 'NoPoll' in cmd and repeatRun:
                continue

            nodename = makeNodeName(cmd['name'])
            node = MainRoot + "/" + nodename

            powerwall_capacity = await powerwall.get_capacity()
            powerwall_charge = await powerwall.get_charge()
            powerwall_energy = await powerwall.get_energy()
            meters = await powerwall.get_meters()
            powerwall_battery_power = meters.battery.get_power()
            powerwall_load_power = meters.load.get_power()
            powerwall_solar_power = meters.solar.get_power()
            powerwall_site_power = meters.site.get_power()
            powerwall_solar_energy_exported = meters.solar.get_energy_exported()
            powerwall_solar_energy_imported = meters.solar.get_energy_imported()
            powerwall_site_energy_imported = meters.site.get_energy_imported()
            powerwall_site_energy_exported = meters.site.get_energy_exported()
            powerwall_load_energy_imported = meters.load.get_energy_imported()
            powerwall_battery_energy_imported = meters.battery.get_energy_imported()
            powerwall_battery_energy_exported = meters.battery.get_energy_exported()

            solar_meter_details = await powerwall.get_meter_solar()
            solar_readings = solar_meter_details.readings
            site_meter_details = await powerwall.get_meter_site()
            site_readings = site_meter_details.readings

            result = eval(cmd['addr'])

            # Some kind of error, probably non fatal
            if result == "":
                continue

            # Some kind of communication error - already logged by sendVS2AndConvert(). Could be serious!
            # Might be a one off though, but if it happens consistently, publish the SystemAlarm.
            if result == "NORESPONSE":
                setSystemAlarm = True
                continue
            else:
                setSystemAlarm = False

            if 'unit' in cmd:
                myUnit = cmd['unit']
            else:
                myUnit = ''

            if cmd['datatype'] == 'float':
                result = format(result, ".2f")
            elif cmd['datatype'] == 'integer':
                if result != None:
                    result = int(result)
                else:
                    result = 0


            logging.info(f'{cmd["name"]}: {result} {cmd["unit"]}')

            if not 'NoPublish' in cmd:
                client.publish(node, result, 1, True)

        if setSystemAlarm:
            client.publish(SystemAlarm, "true", 1, True)
            client.publish(SystemMessage, "Communication Error. No values retrieved", 1, True)
        else:
            repeatRun = True
            client.publish(SystemAlarm, "false", 1, True)
            client.publish(SystemMessage, "Values retrieved", 1, True)

        current_date = datetime.now()
        client.publish(SystemLastSeen, current_date.isoformat(), 1, True)
    else:
        logging.error('Cannot publish values, no communication with Tesla Gateway')
 

#except Exception as e:
#.        logging.error("Unhandled error [" + str(e) + "]")
#       sys.exit(1)

readCmds = [
        { 'addr':'powerwall_charge', 'datatype':'float', 'unit':'%', 'name':'Powerwall Charge'},
        { 'addr':'powerwall_energy', 'datatype':'integer', 'unit':'Wh', 'name':'Powerwall Energy'},
        { 'addr':'powerwall_capacity', 'datatype':'integer', 'unit':'Wh', 'name':'Powerwall Capacity'},
        { 'addr':'site_readings.instant_power', 'datatype':'integer', 'unit':'W', 'name':'Site: Instant Power'},
        { 'addr':'meters.battery.instant_power', 'datatype':'integer', 'unit':'W', 'name':'Battery: Instant Power'},
        { 'addr':'powerwall_battery_power', 'datatype':'float', 'unit':'kW', 'name':'Battery: Power'},
        { 'addr':'meters.load.instant_power', 'datatype':'integer', 'unit':'W', 'name':'Load: Instant Power'},
        { 'addr':'powerwall_load_power', 'datatype':'float', 'unit':'kW', 'name':'Load: Power'},
        { 'addr':'meters.solar.instant_power', 'datatype':'integer', 'unit':'W', 'name':'Solar: Instant Power'},
        { 'addr':'powerwall_solar_power', 'datatype':'float', 'unit':'kW', 'name':'Solar: Power'},
        { 'addr':'site_readings.real_power_a', 'datatype':'integer', 'unit':'W', 'name':'Site: Real Power A'},
        { 'addr':'site_readings.real_power_b', 'datatype':'integer', 'unit':'W', 'name':'Site: Real Power B'},
        { 'addr':'site_readings.real_power_c', 'datatype':'integer', 'unit':'W', 'name':'Site: Real Power C'},
        { 'addr':'powerwall_site_power', 'datatype':'float', 'unit':'kW', 'name':'Site: Power'},
        { 'addr':'solar_readings.energy_exported', 'datatype':'float', 'unit':'Wh', 'name':'Solar: Energy Exported'},
        { 'addr':'powerwall_solar_energy_exported', 'datatype':'float', 'unit':'kWh', 'name':'Solar: Energy Exported (kWh)'},
        { 'addr':'solar_readings.energy_imported', 'datatype':'float', 'unit':'Wh', 'name':'Solar: Energy Imported'},
        { 'addr':'powerwall_solar_energy_imported', 'datatype':'float', 'unit':'kWh', 'name':'Solar: Energy Imported (kWh)'},
        { 'addr':'site_readings.energy_exported', 'datatype':'float', 'unit':'Wh', 'name':'Site: Energy Exported'},
        { 'addr':'site_readings.energy_imported', 'datatype':'float', 'unit':'Wh', 'name':'Site: Energy Imported'},
        { 'addr':'powerwall_site_energy_imported', 'datatype':'float', 'unit':'kWh', 'name':'Site: Energy Imported (kWh)'},
        { 'addr':'powerwall_site_energy_exported', 'datatype':'float', 'unit':'kWh', 'name':'Site: Energy Exported (kWh)'},
        { 'addr':'meters.load.energy_imported', 'datatype':'float', 'unit':'Wh', 'name':'Load: Energy Imported'},
        { 'addr':'powerwall_load_energy_imported', 'datatype':'float', 'unit':'kWh', 'name':'Load: Energy Imported (kWh)'},
        { 'addr':'meters.battery.energy_imported', 'datatype':'integer', 'unit':'Wh', 'name':'Battery: Energy Imported'},
        { 'addr':'powerwall_battery_energy_imported', 'datatype':'float', 'unit':'kWh', 'name':'Battery: Energy Imported (kWh)'},
        { 'addr':'meters.battery.energy_exported', 'datatype':'integer', 'unit':'Wh', 'name':'Battery: Energy Exported'},
        { 'addr':'powerwall_battery_energy_exported', 'datatype':'float', 'unit':'kWh', 'name':'Battery: Energy Exported (kWh)'},

          ]

async def getPowerwallValues():
    logging.debug("Starting get values loop")
    # print("Starting get values loop")
    try:
        while True:
            if pauseRun:
                logging.info("Reading values paused")
            else:
                await publishPowerwallValues()
            logging.info(f"Sleep for {SLEEPTIME}s")
            time.sleep(SLEEPTIME)
    except:
        client.publish(HomieRoot + "$state", payload="lost", qos=1, retain=True)
        logging.error("Stopped by an error " + str(sys.exc_info()[1]))
        client.publish(SystemMessage, "Error: " + str(sys.exc_info()[1]), 1, True)
        raise

def makeNodeName(cmdname):
    nodename = (cmdname.replace(' ','').replace('.','').replace('/','').replace('-','').replace(',','').replace('ä','ae').replace('Ä','Ae').replace('ö','oe').\
            replace('Ö','Oe').replace('ü','ue').replace('Ü','Ue').replace(':','').replace('(','').replace(')','')).lower()
    return nodename

def publishHomieProperty(prop, name, datatype, unit = "", settable = False, defaultValue = "", enumFormat = ""):

    client.publish(prop + '/$name', name, 1, True)
    client.publish(prop + '/$datatype', datatype, 1, True)
    if unit != "":
        client.publish(prop + '/$unit', unit, 1, True)
    if enumFormat != "":
        client.publish(prop + '/$format', enumFormat, 1, True)
    if defaultValue != "":
        client.publish(prop, defaultValue, 1, True)
    if settable:
        client.publish(prop + '/$settable', 'true', 1, True)
        client.subscribe(prop + "/set", 1)

    return True

# Need to handle SIGTERM sent by systemd
def sigterm_handler(signal, frame):
    # save the state here or do whatever you want
    logging.info("Received SIGTERM, exiting")
    client.publish(HomieRoot + "$state", payload="disconnected", qos=1, retain=True)
    client.publish(SystemMessage, "Powerwall received SIGTERM", 1, True)
    sys.exit(0)

signal.signal(signal.SIGTERM, sigterm_handler)

async def main():
    global powerwall, commStarted, client, HomieRoot, SystemRoot, MainRoot
    global SystemMessage, SystemAlarm, SystemLastSeen, SystemSleeptime
    global SystemReread, SystemRelaunch, SystemPause
    
    myformat = "Powerwall: %(message)s"
    logging.basicConfig(format=myformat, level=logLevel)

    try:
        powerwall = Powerwall(GatewayIP)
        await powerwall.login(GatewayPassword, GatewayEmail)
        commStarted = True

        HomieRoot = 'homie/powerwall/'
        client=mqtt.Client(client_id = "powerwall", clean_session = False)

#        client.username_pw_set(username=MQTT_USER,password=MQTT_PASSWORD)
        client.on_connect=connecthandler
        client.on_disconnect=disconnecthandler
#        client.on_message = on_message_to_Tes
        client.will_set(HomieRoot+"$state","lost",qos=2,retain=True)
        client.disconnected =True
        client.connect(MQTT_SERVER,1883,60)


        client.publish(HomieRoot + "$homie", "3.0", 1, True)
        client.publish(HomieRoot+ "$name", "Tesla Powerwall", 1, True)
        client.publish(HomieRoot + "$state", "init", 1, True)
        client.publish(HomieRoot + "$nodes", "Powerwall,system", 1, True)

        SystemRoot = HomieRoot + "system"
        client.publish(SystemRoot + "/$name", "Powerwall Interface", 1, True)
        client.publish(SystemRoot + "/$type", "System", 1, True)
        systemProperties = "lastseen,message,sleeptime,alarm,reread,relaunch,pause"

        client.publish(SystemRoot + "/$properties", systemProperties, 1, True)

        SystemLastSeen = SystemRoot + "/lastseen"
        publishHomieProperty(SystemLastSeen, "PowerwallLastSeen", "datetime")

        SystemMessage = SystemRoot + "/message"
        publishHomieProperty(SystemMessage, "Powerwall Message", "string", settable = True)

        SystemAlarm = SystemRoot + "/alarm"
        publishHomieProperty(SystemAlarm, "Powerwall Alarm", "boolean", settable = True, defaultValue = "false")

        SystemSleeptime = SystemRoot + "/sleeptime"
        publishHomieProperty(SystemSleeptime, "Powerwall Refresh Time (s)", "integer", settable = True, defaultValue = f"{SLEEPTIME}")

        SystemReread = SystemRoot + "/reread"
        publishHomieProperty(SystemReread, "Powerwall Reread all values", "boolean", settable = True, defaultValue = "false")

        SystemRelaunch = SystemRoot + "/relaunch"
        publishHomieProperty(SystemRelaunch, "Powerwall Relaunch Daemon", "boolean", settable = True, defaultValue = "false")

        SystemPause = SystemRoot + "/pause"
        publishHomieProperty(SystemPause, "Powerwall Pause Reading", "boolean", settable = True, defaultValue = "false")

        PowerwallNodes = []
        for cmd in readCmds:
            if 'NoPublish' in cmd:
                continue

            nodename = makeNodeName(cmd['name'])
            PowerwallNodes += [nodename]


        MainRoot = HomieRoot + "Powerwall"
        client.publish(MainRoot + "/$name", "Tesla Powerwall", 1, True)
        client.publish(MainRoot + "/$type", "TeslaGateway", 1, True)
        client.publish(MainRoot + "/$properties", ','.join(PowerwallNodes), 1, True)

        for cmd in readCmds:
            if 'NoPublish' in cmd:
                continue

            mySettable = False
            myUnit = ""
            enumFormat = ""


            nodename = makeNodeName(cmd['name'])
            node = MainRoot + "/" + nodename

            if cmd['datatype'] == 'enum':
                enumFormat = ','.join(cmd['format'].values())
            elif 'format' in cmd:
                enumFormat = cmd['format']

            if 'unit' in cmd:
                myUnit = cmd['unit']
                client.publish(node + "/$unit", cmd['unit'], 1, True)
            if 'settable' in cmd:
                mySettable = True
                settables[nodename] = cmd

            publishHomieProperty(node, cmd['name'], cmd['datatype'], unit = myUnit, settable = mySettable, enumFormat = enumFormat)



        client.loop_start()

        logging.info("Service initialised")
        client.publish(HomieRoot + "$state", "ready", 1, True)
        client.publish(SystemMessage, "Service initialised", 1, True)

        await getPowerwallValues()



    except KeyboardInterrupt:
        client.publish(HomieRoot + "$state", "disconnected", 1, True)
        logging.info("Stopped by user")
        client.publish(SystemMessage, "Powerwall Stopped by user", 1, True)
    except:
        client.publish(HomieRoot + "$state", "lost", 1, True)
        logging.error("Stopped by an error")
        client.publish(SystemMessage, "Error: " + str(sys.exc_info()[1]), 1, True)
        raise
    finally:
        logging.info("Service terminated")

asyncio.run(main())
