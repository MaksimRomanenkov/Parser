package sample

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import kotlin.system.measureTimeMillis

fun main() = runBlocking {
    val concurrentMap: ConcurrentMap<LocalDateTime, Int> = ConcurrentHashMap()
    println("Time read: ${measureTimeMillis { readFiles("input", concurrentMap) }} ms")
    val dir = File("output")
    if (!dir.exists()) dir.mkdirs()
    println("Time write: ${measureTimeMillis {
        writeToFile(
            "output/resultK.txt",
            concurrentMap
        )
    }} ms")
}

suspend fun readFiles(
    inputDir: String,
    concurrentMap: ConcurrentMap<LocalDateTime, Int>
) = coroutineScope {
    Files.walk(Paths.get(inputDir))
        .filter { Files.isRegularFile(it) }
        .forEach {
            launch(Dispatchers.IO) {
                println("start $it + ${Thread.currentThread().name}")
                readFromFile(it, concurrentMap)
                println("end $it")
            }
        }
}

fun readFromFile(
    path: Path,
    concurrentMap: ConcurrentMap<LocalDateTime, Int>
) {
    Files.lines(path)
        .parallel()
        .filter { it.contains("ERROR") }
        .forEach {
            val time = it.substring(0, it.indexOf(';'))
            val key = LocalDateTime.parse(time).truncatedTo(ChronoUnit.HOURS)
            concurrentMap.merge(key, 1, Integer::sum)
        }
}

fun writeToFile(
    toFile: String,
    concurrentMap: ConcurrentMap<LocalDateTime, Int>
) {
    Files.write(
        Path.of(toFile),
        Iterable(
            concurrentMap.keys.stream()
                .sorted()
                .map<CharSequence> {
                    it.format(DateTimeFormatter.ofPattern("yyyy-MM-dd, HH:mm")) +
                            it.plusHours(1).format(DateTimeFormatter.ofPattern("-HH:mm Количество ошибок: ")) +
                            concurrentMap[it]
                }::iterator
        )
    )
}
