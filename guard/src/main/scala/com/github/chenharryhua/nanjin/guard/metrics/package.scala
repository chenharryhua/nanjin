package com.github.chenharryhua.nanjin.guard.metrics

import io.circe.{Decoder, Encoder, Json}
import squants.time.{Frequency, Hertz}

given hertzEncoder: Encoder[Frequency] = Encoder.instance(h => Json.fromDoubleOrNull(h.toHertz))
given hertzDecoder: Decoder[Frequency] = Decoder.decodeDouble.map(Hertz(_))
