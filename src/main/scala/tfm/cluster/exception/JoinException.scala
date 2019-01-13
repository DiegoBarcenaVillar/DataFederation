package tfm.cluster.exception

case class JoinException(message : String = "Error Finding Out Join Columns") extends Exception(message) {

    def getClientMessage: String = {
      message
    }
}
