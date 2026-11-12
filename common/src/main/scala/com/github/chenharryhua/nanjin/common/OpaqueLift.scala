package com.github.chenharryhua.nanjin.common

/** Lifts a typeclass instance from an opaque type's representation type onto the opaque type itself, so
  * instances such as `Show`, `Encoder`, `Decoder` and `Order` need no boilerplate.
  *
  * {{{
  * given Encoder[LogLink] = OpaqueLift.lift[LogLink, String, Encoder]
  * }}}
  *
  * The cast is sound by construction: an opaque type erases to its representation, so the two are the same
  * runtime type. It is still a cast, and the compiler cannot check that `B` really is `A`'s representation.
  */
object OpaqueLift:
  /** Lift any typeclass from representation type `B` to opaque type `A` */
  inline def lift[A, B, TC[_]](using tc: TC[B]): TC[A] =
    tc.asInstanceOf[TC[A]] // scalafix:ok
end OpaqueLift
