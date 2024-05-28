package mtest.http

final class BooleanList(private[this] var state: LazyList[Boolean]) {
  def get: Boolean = state match {
    case hd #:: rest =>
      state = rest
      hd
    case _ => false
  }
}

object BooleanList {
  def apply(ll: LazyList[Boolean]): BooleanList = new BooleanList(ll)
}
