#!/usr/bin/env python


from abc import ABC, abstractmethod
from typing import Mapping

from hardware import *
import log


## emulates a compiled program
class Program():

    def __init__(self, instructions):
        self._instructions = self.expand(instructions)

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
        return "Program({instructions})".format(instructions=self._instructions)


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
            # print(pair)
            pcb = pair['pcb']
            instruction = pair['instruction']
            self._currentPCB = pcb
            self._device.execute(instruction)

    def __repr__(self):
        return "IoDeviceController for {deviceID} running: {currentPCB} waiting: {waiting_queue}".format(
            deviceID=self._device.deviceId, currentPCB=self._currentPCB, waiting_queue=self._waiting_queue)


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
        path = irq.parameters["path"]
        pcb = self.kernel.pcbTable.newPCB(irq.parameters["prioridad"])
        self.kernel.loader.load(pcb, path)
        if self.kernel.cpuFree():
            self.kernel.jumpReadyQueue(pcb)
        elif self.kernel.scheduler.mustExpropiate(pcb, self.kernel.pcbTable.running):
            log.logger.info(f'Expropiando: {self.kernel.pcbTable.running} Por: {pcb}')
            self.kernel.dispatcher.save(self.kernel.pcbTable.running)
            self.kernel.sendToReadyQueue(self.kernel.pcbTable.running)
            self.kernel.jumpReadyQueue(pcb)
        else:
            self.kernel.sendToReadyQueue(pcb)
            log.logger.info(repr(self.kernel.scheduler))


class KillInterruptionHandler(AbstractInterruptionHandler):

    def execute(self, irq):
        runningPCB = self.kernel.pcbTable.running
        self.kernel.dispatcher.save(runningPCB)
        runningPCB.status = State.Terminated

        log.logger.info("Program finished")
        self.kernel.pcbTable.running = None
        self.kernel.runNext()
        log.logger.info(HARDWARE.memory)


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


class TimeOutInterruptionHandler(AbstractInterruptionHandler):
    def execute(self, irq):
        self.kernel.dispatcher.save(self.kernel.pcbTable.running)
        self.kernel.sendToReadyQueue(self.kernel.pcbTable.running)
        self.kernel.runNext()


class PCB():
    def __init__(self, pId, prioridad):
        self._PCBId = pId
        self._status = State.New
        self._baseDir = 0
        self._limit = 0
        self._pc = 0
        self._prioridad = prioridad
        self._waitingTime = 0
        self._tempPriority = self._prioridad

    @property
    def pcbId(self):
        return self._PCBId

    def __repr__(self):
        return f'(id: {repr(self.pcbId)} - pr: {self.tempPriority} - wt: {self.waitingTime})'

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

    @property
    def prioridad(self):
        return self._prioridad

    @prioridad.setter
    def prioridad(self, value):
        self._prioridad = value

    @pc.setter
    def pc(self, value):
        self._pc = value

    @status.setter
    def status(self, value):
        self._status = value

    @property
    def waitingTime(self):
        return self._waitingTime

    @waitingTime.setter
    def waitingTime(self, value):
        self._waitingTime = value

    @property
    def tempPriority(self):
        return self._tempPriority

    @tempPriority.setter
    def tempPriority(self, value):
        self._tempPriority = value

    def aging(self):
        if self.tempPriority > 1:
            self.tempPriority = self.tempPriority - 1

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

    def newPCB(self, prioridad):
        pId = self.getNewPID()
        pcb = PCB(pId, prioridad)
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


class SchedulerFCFS():

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
        self.readyQueue.append(proc)

    def isEmpty(self):
        return not self._readyQueue

    def getNextProcess(self):
        if self._readyQueue:
            nextPCB = self.readyQueue.pop(0)
            return nextPCB
        else:
            log.logger.info("readyQueue is empty")
            return None

    def mustExpropiate(self, pcb1, pcb2):
        return False


class SchedulerRoundRobin():

    def __init__(self, quantum):
        self._readyQueue = []
        HARDWARE.timer.quantum = quantum

    def __len__(self):
        return len(self.readyQueue)

    def __repr__(self):
        return repr(self.readyQueue)

    @property
    def readyQueue(self):
        return self._readyQueue

    def addProcess(self, proc):
        self.readyQueue.append(proc)

    def isEmpty(self):
        return not self._readyQueue

    def getNextProcess(self):
        if self._readyQueue:
            nextPCB = self.readyQueue.pop(0)
            return nextPCB
        else:
            log.logger.info("readyQueue is empty")
            return None

    def mustExpropiate(self, pcb1, pcb2):
        return False

class SchedulerPriorityNoExpropiativo(SchedulerFCFS):

    def __init__(self, limit):
        self._readyQueue = []
        HARDWARE.clock.addSubscriber(self)
        self._agingLimit = limit

    def __len__(self):
        return len(self.readyQueue)

    def __repr__(self):
        return repr(self.readyQueue)

    @property
    def readyQueue(self):
        return self._readyQueue

    def tick(self, tickNbr):
        for pcb in self.readyQueue:
            pcb.waitingTime = pcb.waitingTime + 1
        log.logger.info(self.readyQueue)
        for pcb in self.readyQueue:
            self.agingIfApply(pcb)
        sorted(self.readyQueue, key=lambda x: x.tempPriority)

    def agingIfApply(self, pcb):
        if pcb.waitingTime == self.agingLimit:
            pcb.aging()
            pcb.waitingTime = 0

    @property
    def agingLimit(self):
        return self._agingLimit

    def addProcess(self, proc):
        procesosImportantes = []
        log.logger.info(f'Ready queue is empty: {self.isEmpty()}')
        while not self.isEmpty() and (self.readyQueue[0].prioridad < proc.prioridad):
            procesosImportantes.append(self.readyQueue.pop(0))
            log.logger.info(f'proc: {proc} - procesosImportantes: {procesosImportantes}')
        procesosImportantes.append(proc)
        self.readyQueue = procesosImportantes + self.readyQueue
        log.logger.info(f'Added {proc} - Result: {self.readyQueue}')

    @readyQueue.setter
    def readyQueue(self, value):
        self._readyQueue = value    

    def isEmpty(self):
        #return not self._readyQueue
        return len(self._readyQueue) == 0

    def getNextProcess(self):
        if self._readyQueue:
            nextPCB = self.readyQueue.pop(0)
            return nextPCB
        else:
            log.logger.info("readyQueue is empty")
            return None    

    def mustExpropiate(self, pcb1, pcb2):
        return False


class SchedulerPriorityExpropiativo():

    def __init__(self, limit):
        self._readyQueue = []
        HARDWARE.clock.addSubscriber(self)
        self._agingLimit = limit

    def __len__(self):
        return len(self.readyQueue)

    def __repr__(self):
        return repr(self.readyQueue)

    @property
    def readyQueue(self):
        return self._readyQueue

    def tick(self, tickNbr):
        for pcb in self.readyQueue:
            pcb.waitingTime = pcb.waitingTime + 1
        log.logger.info(self.readyQueue)
        for pcb in self.readyQueue:
            self.agingIfApply(pcb)
        sorted(self.readyQueue, key=lambda x: x.tempPriority)

    def agingIfApply(self, pcb):
        if pcb.waitingTime == self.agingLimit:
            pcb.aging()
            pcb.waitingTime = 0

    @property
    def agingLimit(self):
        return self._agingLimit

    def addProcess(self, proc):
        procesosImportantes = []
        log.logger.info(f'Ready queue is empty: {self.isEmpty()}')
        while not self.isEmpty() and (self.readyQueue[0].prioridad < proc.prioridad):
            procesosImportantes.append(self.readyQueue.pop(0))
            log.logger.info(f'proc: {proc} - procesosImportantes: {procesosImportantes}')
        procesosImportantes.append(proc)
        self.readyQueue = procesosImportantes + self.readyQueue
        log.logger.info(f'Added {proc} - Result: {self.readyQueue}')

    @readyQueue.setter
    def readyQueue(self, value):
        self._readyQueue = value

    def isEmpty(self):
        #return not self._readyQueue
        return len(self._readyQueue) == 0

    def getNextProcess(self):
        if self._readyQueue:
            nextPCB = self.readyQueue.pop(0)
            return nextPCB
        else:
            log.logger.info("readyQueue is empty")
            return None

    def mustExpropiate(self, pcb1, pcb2):
        return pcb1.prioridad < pcb2.prioridad


class Loader():
    def __init__(self, memoryManager, fileSystem):
        self._memoryManager = memoryManager
        self._fileSystem = fileSystem

    @property
    def memoryManager(self):
        return self._memoryManager    

    def load(self, pcb, path):
        instructions = self._fileSystem.read(path)
        frameSize = self._memoryManager.frameSize
        lengthPrg = len(instructions)
        tlb = self._memoryManager.createTLB(pcb, lengthPrg)
        for pageInfo in tlb:
            page = pageInfo.page
            frame = pageInfo.frame
            dirBase = frame * frameSize
            start = page * frameSize
            end = start + frameSize
            if end > lengthPrg:
                end = lengthPrg
            for i in range(start, end):
                instruction = instructions[i]
                HARDWARE.memory.write(dirBase, instruction)
                dirBase = dirBase + 1
        log.logger.info(HARDWARE.memory)

class MemoryManager():

    def __init__(self):
        self._frameSize = HARDWARE.mmu.frameSize
        self._globalTlb = []
        self._frames = []

        framesNumber = HARDWARE.memory.size
        for i in range(0, framesNumber - 1):
            self._frames.append(i)

    @property
    def frameSize(self):
        return self._frameSize

    def createTLB(self, pcb, progLength):
        tlb = []
        pages = self.countPages(progLength)
        frames = self.getFrames(pages)
        for page in range(0, pages):
            frame = frames[page]
            pageInfo = Page(pcb.pcbId, page, frame)
            tlb.append(pageInfo)
            self._globalTlb.append(pageInfo) 
        return tlb

    def countPages(self, length):
        pageCount = length // self.frameSize
        if length % self.frameSize > 0:
            pageCount = pageCount + 1
        return pageCount    

    def getFrames(self, pages):
        frames = []
        for i in range(0, pages):
            frames.append(self._frames.pop(0))
        return frames    

    def initializeMMUFor(self, pcb):
        HARDWARE.mmu.resetTLB()
        tlb = self.getTLB(pcb)
        for pageInfo in tlb:
            HARDWARE.mmu.setPageFrame(pageInfo.page, pageInfo.frame)    

    def getTLB(self, pcb):
        tlb = []
        for pageInfo in self._globalTlb:
            if pageInfo.id == pcb.pcbId:
                tlb.append(pageInfo)
        return tlb        



class Page():
    
    def __init__(self, id, page, frame):
        self._id = id
        self._page = page
        self._frame = frame

    @property
    def id(self):
        return self._id

    @property
    def page(self):
        return self._page

    @property
    def frame(self):
        return self._frame

class Dispatcher():

    def __init__(self, memoryManager):
        self._memoryManager = memoryManager

    @property
    def memoryManager(self):
        return self._memoryManager    

    def save(self, pcb):
        pcb.pc = HARDWARE.cpu.pc
        HARDWARE.cpu.pc = -1
        log.logger.info("Dispatcher save {pId}".format(pId=pcb.pcbId))

    def load(self, pcb):
        HARDWARE.cpu.pc = pcb.pc
        HARDWARE.mmu.baseDir = pcb.baseDir
        #El dispatcher se encargará de resetear el timer
        HARDWARE.timer.reset()
        self._memoryManager.initializeMMUFor(pcb)
        log.logger.info("Dispatcher load {pId}".format(pId=pcb.pcbId))

    # emulates the core of an Operative System


class FileSystem():
    

    def __init__(self):
        self._files = {}

    @property
    def files(self):
        return self._files    

    def write(self, path, prog):
        self._files.update({path: prog.instructions})
    
    def read(self, path):
        ##log.logger.info(f"{self._files}, path: {path}, {self._files.get(path)}")
        return self.files[path]   


class Kernel():

    def __init__(self):
        ## iniciamos el frame size
        HARDWARE.mmu.frameSize = 8
        ## setup interruption handlers
        killHandler = KillInterruptionHandler(self)
        HARDWARE.interruptVector.register(KILL_INTERRUPTION_TYPE, killHandler)

        ioInHandler = IoInInterruptionHandler(self)
        HARDWARE.interruptVector.register(IO_IN_INTERRUPTION_TYPE, ioInHandler)

        ioOutHandler = IoOutInterruptionHandler(self)
        HARDWARE.interruptVector.register(IO_OUT_INTERRUPTION_TYPE, ioOutHandler)

        newHandler = NewInterruptionHandler(self)
        HARDWARE.interruptVector.register(NEW_INTERRUPTION_TYPE, newHandler)

        timeOutHandler = TimeOutInterruptionHandler(self)
        HARDWARE.interruptVector.register(TIMEOUT_INTERRUPTION_TYPE, timeOutHandler)

        ## controls the Hardware's I/O Device
        self._ioDeviceController = IoDeviceController(HARDWARE.ioDevice)

        ## setup interuption vector
        self._interruptVector = HARDWARE.interruptVector

        self._memoryManager = MemoryManager()
        self._pcbTable = PCBTable()
        self._scheduler = SchedulerRoundRobin(4)
        self._fileSystem = FileSystem()
        self._loader = Loader(self._memoryManager, self._fileSystem)
        self._dispatcher = Dispatcher(self._memoryManager)

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
    def scheduler(self):
        return self._scheduler

    @property
    def dispatcher(self):
        return self._dispatcher

    @property
    def interruptVector(self):
        return self._interruptVector

    @property
    def fileSystem(self):    
        return self._fileSystem

    @property
    def memoryManager(self):
        return self._memoryManager    

    def cpuFree(self):
        return not self.pcbTable.hasRunning()

    def jumpReadyQueue(self, pcb):
        pcb.status = State.Running
        self.pcbTable.running = pcb
        self.dispatcher.load(pcb)

    def sendToReadyQueue(self, pcb):
        pcb.status = State.Ready
        self.scheduler.addProcess(pcb)

    def runNext(self):
        nextPCB = self.scheduler.getNextProcess()
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
    def run(self, path, prioridad):
        newIRQ = IRQ(NEW_INTERRUPTION_TYPE, {"path" :path, "prioridad":prioridad})
        self._interruptVector.handle(newIRQ)

    def __repr__(self):
        return "Kernel "




