from hardware import *
from so import *
import log


##
##  MAIN 
##
if __name__ == '__main__':
    log.setupLogger()
    log.logger.info('Starting emulator')

    ## setup our hardware and set memory size to 25 "cells"
    HARDWARE.setup(50)

    ## Switch on computer
    HARDWARE.switchOn()

    ## new create the Operative System Kernel
    # "booteamos" el sistema operativo
    kernel = Kernel()

   # Ahora vamos a guardar los programas en el FileSystem
   ##################
    prg1 = Program([ASM.CPU(2), ASM.IO(), ASM.CPU(3), ASM.IO(), ASM.CPU(2)])
    prg2 = Program([ASM.CPU(7)])
    prg3 = Program([ASM.CPU(8), ASM.IO(), ASM.CPU(1)])
    
    kernel.fileSystem.write("c:/prg1.exe", prg1)
    kernel.fileSystem.write("c:/prg2.exe", prg2)
    kernel.fileSystem.write("c:/prg3.exe", prg3)

    # ejecutamos los programas a partir de un “path” (con una prioridad x)
    kernel.run("c:/prg1.exe", 5)
    kernel.run("c:/prg2.exe", 4)
    kernel.run("c:/prg3.exe", 1)




