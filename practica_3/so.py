#!/usr/bin/env python

#from winreg import DisableReflectionKey
from hardware import *
import log



## emulates a compiled program
class Program():

    def __init__(self, name, instructions):
        self._name = name
        self._instructions = self.expand(instructions)

    @property
    def name(self):
        return self._name

    @property
    def instructions(self):
        return self._instructions

    def addInstr(self, instruction):
        self._instructions.append(instruction)

    def expand(self, instructions):
        expanded = []
        for i in instructions:
            if isinstance(i, list):
                ## is a list of instructions
                expanded.extend(i)
            else:
                ## a single instr (a String)
                expanded.append(i)

        ## now test if last instruction is EXIT
        ## if not... add an EXIT as final instruction
        last = expanded[-1]
        if not ASM.isEXIT(last):
            expanded.append(INSTRUCTION_EXIT)

        return expanded

    def __repr__(self):
        return "Program({name}, {instructions})".format(name=self._name, instructions=self._instructions)


## emulates an Input/Output device controller (driver)
class IoDeviceController():

    def __init__(self, device):
        self._device = device
        self._waiting_queue = []
        self._currentPCB = None

    def runOperation(self, pcb, instruction):
        pair = {'pcb': pcb, 'instruction': instruction}
        # append: adds the element at the end of the queue
        self._waiting_queue.append(pair)
        # try to send the instruction to hardware's device (if is idle)
        self.__load_from_waiting_queue_if_apply()

    def getFinishedPCB(self):
        finishedPCB = self._currentPCB
        self._currentPCB = None
        self.__load_from_waiting_queue_if_apply()
        return finishedPCB

    def __load_from_waiting_queue_if_apply(self):
        if (len(self._waiting_queue) > 0) and self._device.is_idle:
            ## pop(): extracts (deletes and return) the first element in queue
            pair = self._waiting_queue.pop(0)
            #print(pair)
            pcb = pair['pcb']
            instruction = pair['instruction']
            self._currentPCB = pcb
            self._device.execute(instruction)


    def __repr__(self):
        return "IoDeviceController for {deviceID} running: {currentPCB} waiting: {waiting_queue}".format(deviceID=self._device.deviceId, currentPCB=self._currentPCB, waiting_queue=self._waiting_queue)

## emulates the  Interruptions Handlers
class AbstractInterruptionHandler():
    def __init__(self, kernel):
        self._kernel = kernel

    @property
    def kernel(self):
        return self._kernel

    def execute(self, irq):
        log.logger.error("-- EXECUTE MUST BE OVERRIDEN in class {classname}".format(classname=self.__class__.__name__))


class NewInterruptionHandler(AbstractInterruptionHandler):

    def execute(self, irq):
        prog = irq.parameters
        baseDir = self.kernel.loader.load(prog)
        limit = len(prog.instructions) -1
        pcb = self.kernel.pcbTable.newPCB(baseDir, limit)
        if not self.kernel.pcbTable.hasRunning():
            self.kernel.dispatcher.load(pcb)
            self.kernel.pcbTable.running = pcb
            pcb.status = State.Running
        else:
            pcb.status = State.Ready
            self.kernel.readyQueue.addProcess(pcb)
            log.logger.info(repr(self.kernel.readyQueue))



class KillInterruptionHandler(AbstractInterruptionHandler):

    def execute(self, irq):
        runningPCB = self.kernel.pcbTable.running
        self.kernel.dispatcher.save(runningPCB)
        runningPCB.status = State.Terminated
        log.logger.info("Program finished")
        self.kernel.pcbTable.running = None
        self.kernel.runNext()


class IoInInterruptionHandler(AbstractInterruptionHandler):

    def execute(self, irq):
        runningPCB = self.kernel.pcbTable.running
        self.kernel.dispatcher.save(runningPCB)
        runningPCB.status = State.Waiting
        self.kernel.pcbTable.running = None
        self.kernel.ioDeviceController.runOperation(runningPCB, irq)
        log.logger.info(self.kernel.ioDeviceController)
        self.kernel.runNext()



class IoOutInterruptionHandler(AbstractInterruptionHandler):

    def execute(self, irq):
        pcb = self.kernel.ioDeviceController.getFinishedPCB()
        log.logger.info(self.kernel.ioDeviceController)
        if self.kernel.cpuFree():
            self.kernel.jumpReadyQueue(pcb)
        else:
            self.kernel.sendToReadyQueue(pcb)


class PCB():
    def __init__(self, pId, baseDir, limit):
        self._PCBId = pId
        self._status = State.New
        self._baseDir = baseDir
        self._limit = limit
        self._pc = 0
    
    @property
    def pcbId(self):
        return self._PCBId

    def __repr__(self):
        return "id: "+repr(self.pcbId)
    
    @property
    def status(self):
        return self._status

    @property
    def baseDir(self):
        return self._baseDir

    @property
    def pcbId(self):
        return self._PCBId     

    @property
    def limit(self):
        return self._limit

    @property
    def pc(self):
        return self._pc

    @pc.setter
    def pc(self, value):
        self._pc = value              

    @status.setter
    def status(self, value):
        self._status = value

class State():

     New = "new"
     Ready = "ready"
     Waiting = "waiting"
     Running = "running"
     Terminated = "terminated"


class PCBTable():

    def __init__(self):
        self._table = []
        self._running = None

    def newPCB(self, baseDir, limit):
        pId = self.getNewPID()
        pcb = PCB(pId, baseDir, limit)
        self.table.append(pcb)
        return pcb

    def hasRunning(self):
        return self.running != None      

    def getNewPID(self):
        pid = len(self._table)
        return pid

    @property
    def table(self):
        return self._table    

    @property
    def running(self):
        return self._running

    @running.setter
    def running(self, value):
        self._running = value        

class ReadyQueue():

    def __init__(self):
        self._readyQueue = []

    def __len__(self):
        return len(self.readyQueue)

    def __repr__(self):
        return repr(self.readyQueue)

    @property
    def readyQueue(self):
        return self._readyQueue

    def addProcess(self, proc):
        self._readyQueue.append(proc)

    def isEmpty(self):
        return not self._readyQueue

    def getNextProcess(self):
        if self._readyQueue:
            nextPCB = self.readyQueue.pop(0)
            return nextPCB
        else:
            log.logger.info("readyQueue is empty")
            return None

class Loader():
    def __init__(self):
        self._actualDir = 0

    def load(self, program):
        baseDir = self._actualDir
        instructions = program.instructions
        for i in instructions:
            HARDWARE.memory.write(self._actualDir, i)
            self._actualDir = self._actualDir + 1
        log.logger.info(HARDWARE.memory)
        return baseDir    


class Dispatcher():

    def save(self, pcb):
        pcb.pc = HARDWARE.cpu.pc
        HARDWARE.cpu.pc = -1
        log.logger.info("Dispatcher save {pId}".format(pId = pcb.pcbId) )

    def load(self, pcb):
        HARDWARE.cpu.pc = pcb.pc
        HARDWARE.mmu.baseDir = pcb.baseDir
        log.logger.info("Dispatcher load {pId}".format(pId = pcb.pcbId) )    


# emulates the core of an Operative System
class Kernel():

    def __init__(self):
        ## setup interruption handlers
        killHandler = KillInterruptionHandler(self)
        HARDWARE.interruptVector.register(KILL_INTERRUPTION_TYPE, killHandler)

        ioInHandler = IoInInterruptionHandler(self)
        HARDWARE.interruptVector.register(IO_IN_INTERRUPTION_TYPE, ioInHandler)

        ioOutHandler = IoOutInterruptionHandler(self)
        HARDWARE.interruptVector.register(IO_OUT_INTERRUPTION_TYPE, ioOutHandler)

        newHandler = NewInterruptionHandler(self)
        HARDWARE.interruptVector.register(NEW_INTERRUPTION_TYPE, newHandler)

        ## controls the Hardware's I/O Device
        self._ioDeviceController = IoDeviceController(HARDWARE.ioDevice)

        ## setup interuption vector
        self._interruptVector = HARDWARE.interruptVector

        self._pcbTable = PCBTable()
        self._loader = Loader()
        self._readyQueue = ReadyQueue()
        self._dispatcher = Dispatcher()
    
    @property
    def ioDeviceController(self):
        return self._ioDeviceController

    @property
    def pcbTable(self):
        return self._pcbTable

    @property
    def loader(self):
        return self._loader

    @property
    def readyQueue(self):
        return self._readyQueue

    @property
    def dispatcher(self):
        return self._dispatcher

    @property
    def interruptVector(self):
        return self._interruptVector               

    def cpuFree(self):
        return not self.pcbTable.hasRunning()

    def jumpReadyQueue(self, pcb):
        pcb.status = State.Running
        self.pcbTable.running = pcb
        self.dispatcher.load(pcb)

    def sendToReadyQueue(self, pcb):
        pcb.status = State.Ready
        self.readyQueue.addProcess(pcb)    

    def runNext(self):
        nextPCB = self.readyQueue.getNextProcess()
        if not nextPCB is None:
            nextPCB.status = State.Running
            self.pcbTable.running = nextPCB
            self.dispatcher.load(nextPCB)

    def load_program(self, program):
        # loads the program in main memory
        progSize = len(program.instructions)
        for index in range(0, progSize):
            inst = program.instructions[index]
            HARDWARE.memory.write(index, inst)

    ## emulates a "system call" for programs execution
    def run(self, program):
        newIRQ = IRQ(NEW_INTERRUPTION_TYPE, program)
        self._interruptVector.handle(newIRQ)

    def __repr__(self):
        return "Kernel "








