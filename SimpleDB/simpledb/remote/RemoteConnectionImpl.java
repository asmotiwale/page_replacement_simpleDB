package simpledb.remote;

import simpledb.tx.Transaction;
import simpledb.buffer.BufferMgrStatistics;
import simpledb.file.*;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * The RMI server-side implementation of RemoteConnection.
 * @author Edward Sciore
 */
@SuppressWarnings("serial") 
class RemoteConnectionImpl extends UnicastRemoteObject implements RemoteConnection {
   private Transaction tx;
   public static Statistics stats;
   
   public static BufferMgrStatistics bs;
   
   /**
    * Creates a remote connection
    * and begins a new transaction for it.
    * @throws RemoteException
    */
   RemoteConnectionImpl() throws RemoteException {
      tx = new Transaction();
   }
   
   /**
    * Creates a new RemoteStatement for this connection.
    * @see simpledb.remote.RemoteConnection#createStatement()
    */
   public RemoteStatement createStatement() throws RemoteException {
      return new RemoteStatementImpl(this);
   }
   
   /**
    * Closes the connection.
    * The current transaction is committed.
    * @see simpledb.remote.RemoteConnection#close()
    */
   public void close() throws RemoteException {
      tx.commit();
   }
   
// The following methods are used by the server-side classes.
   
   /**
    * Returns the transaction currently associated with
    * this connection.
    * @return the transaction associated with this connection
    */
   Transaction getTransaction() {  
      return tx;
   }
   
   /**
    * Commits the current transaction,
    * and begins a new one.
    */
   void commit() {
	  tx.commit();
	  stats = FileMgrStatistics.getFileStatistics();
	  System.out.println();
	  System.out.println("### FILE STATISTICS ###");
	  System.out.println("Blocks read: "+stats.blk_read);
	  System.out.println("Blocks written: "+stats.blk_written);
	  System.out.println();
	  System.out.println("### BUFFER STATISTICS ###");
	  bs.getBufferStatistics();
      tx = new Transaction();
   }
   
   /**
    * Rolls back the current transaction,
    * and begins a new one.
    */
   void rollback() {
      tx.rollback();
      System.out.println();
	  System.out.println("### FILE STATISTICS ###");
      System.out.println("Blocks read: "+stats.blk_read);
	  System.out.println("Blocks written: "+stats.blk_written);
	  System.out.println();
	  System.out.println("### BUFFER STATISTICS ###");
	  bs.getBufferStatistics();
      tx = new Transaction();
   }
}

