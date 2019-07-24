package com.github.chenharryhua.nanjin

package object kafka
    extends KafkaMessageBitraverse with Fs2MessageBitraverse with AkkaMessageBitraverse {}
