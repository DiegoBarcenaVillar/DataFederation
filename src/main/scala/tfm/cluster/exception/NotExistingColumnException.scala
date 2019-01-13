package tfm.cluster.exception

case class NotExistingColumnException(message : String = "Not Found Column Names"
                                 , columnsNotFound : Array[String]) extends Exception(message) {

      def getClientMessage : String = {
        val columnNamesString = columnsNotFound.reduce((x,y)=> x.concat(", ").concat(y))

        message.concat(": ").concat(columnNamesString)
      }
}
