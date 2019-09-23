package com.ippontech.kafkatutorials.events

import java.util.*

fun main(args: Array<String>) {
    GenerateUUIDs().generate()
}

class GenerateUUIDs () {
    private val size=3
    val ids = Array<UUID>(size,{ UUID.randomUUID()})
    fun generate(){
        for (i in 0..size-1) {
            ids[i] = UUID.randomUUID()
        }
        println(Arrays.toString(ids))
    }

}