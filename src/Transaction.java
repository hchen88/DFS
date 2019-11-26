public class Transaction {

    public enum Operation{ WRITE, DELETE};
    public enum Vote { YES, NO};
    Long TransactionId;
    Vote vote;
    String fileName;
    Long pageIndex;


    public Transaction() {

    }



}
