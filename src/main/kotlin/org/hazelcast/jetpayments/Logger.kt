package org.hazelcast.jetpayments

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract

/*
 * A very simple Logger class that allows for some simple formatting goodies.
 */
open class Logger(
    private val label: String, private val addTimestamp: Boolean = true
) : java.io.Serializable {

    /*
     * Private methods
     */

    private val formatter: DateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")

    /*
     * This is the main private function for logging text, with all the (optional)
     * embellishments. Addds timestamp if requests, like any good logger.
     */
    private fun logVararg(
        vararg strings: String,
        center: Boolean = false, // Center the lines, if boxed?
        style: Style = Style.NONE, // What style? None, emphasis, boxed?
        boxStyle: BoxStyle = BoxStyle.SINGLE, // Thick, double or single box?
    ) {
        require(strings.isNotEmpty())
        require(strings.size == 1 || style == Style.BOXED)

        val prefix = if (addTimestamp) {
            "${getFormattedTime()} [${label}]: "
        } else "[${label}]: "

        when (style) {
            Style.NONE -> println("$prefix${strings.first()}")

            Style.EMPHASIS -> println("$prefix/=> ${strings.first()} <=/")

            Style.BOXED -> {
                val chars = boxStyle.boxChars
                val maxlen = strings.maxOf { it.numCodepoints() }
                val horz = chars.horz.repeat(maxlen + 2)
                val top = "${chars.tleft}$horz${chars.tright}"
                println("$prefix$top")
                val middle = strings.map { str ->
                    val len = str.numCodepoints()
                    val leftpad = if (center) (maxlen - len) / 2 else 0
                    val rightpad =
                        if (center) maxlen - len - leftpad else maxlen - len
                    "${chars.vert}${" ".repeat(leftpad)} $str ${" ".repeat(rightpad)}${chars.vert}"
                }
                middle.forEach { println("$prefix$it") }
                val bottom = "${chars.bleft}$horz${chars.bright}"
                println("$prefix$bottom")
            }
        }
    }

    /*
     * Overridable methods
     */

    protected open fun getFormattedTime() = LocalDateTime.now().format(formatter)!!

    /*
     * Public properties and methods
     */

    enum class Style {
        NONE, EMPHASIS, BOXED,
    }

    enum class BoxStyle {
        SINGLE, THICK, DOUBLE;

        data class BoxChars(
            val vert: String,
            val horz: String,
            val tleft: String,
            val tright: String,
            val bleft: String,
            val bright: String,
        )

        val boxChars: BoxChars
            get() = when (this) {
                SINGLE -> BoxChars("│", "─", "┌", "┐", "└", "┘")
                THICK -> BoxChars("┃", "━", "┏", "┓", "┗", "┛")
                DOUBLE -> BoxChars("║", "═", "╔", "╗", "╚", "╝")
            }
    }

    // Log a single line of text.
    fun log(s: String) = logVararg(s)

    // Log a single line of text, with some decorations to make it stand out.
    fun logEmphasis(s: String) = logVararg(s, style = Style.EMPHASIS)

    // Log multiple lines of text, with a box around it.
    fun logBoxed(
        vararg strings: String,
        boxStyle: BoxStyle = BoxStyle.SINGLE,
        center: Boolean = true,
    ) = logVararg(
        *strings,
        style = Style.BOXED,
        boxStyle = boxStyle,
        center = center,
    )
}

fun String.log(logger: Logger) = logger.log(this)
fun Collection<String>.log(logger: Logger) = this.forEach { it.log(logger) }
fun Collection<String>.logBoxed(
    logger: Logger,
    boxStyle: Logger.BoxStyle = Logger.BoxStyle.SINGLE,
    center: Boolean = false,
) = logger.logBoxed(
    *this.toTypedArray(), boxStyle = boxStyle, center = center
)

@OptIn(ExperimentalContracts::class)
inline fun logBoxed(
    logger: Logger,
    boxStyle: Logger.BoxStyle = Logger.BoxStyle.SINGLE,
    center: Boolean = true,
    block: MutableList<String>.() -> Unit,
) {
    contract { callsInPlace(block, InvocationKind.AT_MOST_ONCE) }
    buildList {
        apply(block)
    }.toList().logBoxed(logger, boxStyle, center)
}
