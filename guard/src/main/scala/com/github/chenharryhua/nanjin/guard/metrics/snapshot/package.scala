package com.github.chenharryhua.nanjin.guard.metrics.snapshot

import io.circe.{Decoder, Encoder, Json}
import org.apache.commons.lang3.StringUtils
import squants.time.{Frequency, Hertz}

given hertzEncoder: Encoder[Frequency] = Encoder.instance(h => Json.fromDoubleOrNull(h.toHertz))
given hertzDecoder: Decoder[Frequency] = Decoder.decodeDouble.map(Hertz(_))

private val space2: String = StringUtils.SPACE * 2
private val space4: String = StringUtils.SPACE * 4

private val decimalFormat: "#,###" = "#,###"
