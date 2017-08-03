// Comment written
package simpledb.buffer;

import java.util.HashMap;
import java.util.Map;

import simpledb.file.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   
   // Creating a map for mapping the block number with the buffer
   HashMap<String,Buffer> bufferPoolMap =new HashMap<String,Buffer>();
   //ArrayList<Buffer> new_pool = new ArrayList<Buffer>();
   private int total_buffers;
   

   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      total_buffers = numbuffs;
      for (int i=0; i<numbuffs; i++)
         bufferpool[i] = new Buffer();
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
//      for (Buffer buff : bufferpool)
//         if (buff.isModifiedBy(txnum))
//         buff.flush();
     for (Buffer buff : bufferPoolMap.values())
           if (buff.isModifiedBy(txnum))
           buff.flush();
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
     // Checking if the block is present in the buffer pool map.
      Buffer buff = findExistingBuffer(blk);
     
      System.out.println("Hash map contents:");
      System.out.println(bufferPoolMap);
      System.out.println("###################");

      
      // Checking if the block is not pinned to a buffer and if there is no space in the new buffer pool
      if(buff == null){
    	  
        // Checking if there is space in the buffer pool map to add the new buffer
        if(bufferPoolMap.size() < total_buffers){
          // There is space available in the buffer

        // Create a new buffer and pin it to the block
              buff = new Buffer();
              buff.assignToBlock(blk);
              
              // Add the new buffer to the bufferPoolMap
              bufferPoolMap.put(buff.block().fileName()+"."+buff.block().number(), buff);
              numAvailable -= 1;
              buff.pin();
              return buff;
          
        }
        else{
             // There is no space available in the buffer
          
             // Get the most recently modified buffer.
               buff = chooseMostRecentlyModifiedBuffer();
               
               // Checking if we get any buffers using MRM policy
               if (buff == null){
                 
                 // If we do not get any buffers, we simply select any of the unpinned buffers and replace it.
                   buff = chooseUnpinnedBuffer();
                   if(buff == null){
                     return null;
                   }
               }
              // Remove the buffer selected from the buffer pool map and the new_pool as well. 
               bufferPoolMap.remove(buff.block().fileName()+"."+buff.block().number());
               
               // Assign the current block to a buffer.
               buff.assignToBlock(blk);
               
               /*
                * Add the current buffer to the map and new_pool
                */
               bufferPoolMap.put(buff.block().fileName()+"."+buff.block().number(), buff);
               
               // Pin the buffer and return it.
               buff.pin();
               buff.block().fileName();
               return buff;
               
        }
      }
      else{
        System.out.println("Matching buffer = " +buff);
        // The buffer is present in the buffer pool map, so we just pin it and return it.
        buff.pin();
        numAvailable--;
        return buff;
      }
      
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
        throw new BufferAbortException();
         //return null;
      
      buff.assignToNew(filename, fmtr);
      bufferPoolMap.put(buff.block().fileName()+"."+buff.block().number(), buff);
      
      numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk) {
     // Check if the block is already assigned a buffer in the map.
     return bufferPoolMap.get(blk.fileName()+"."+blk.number());
//      for (Buffer buff : bufferpool) {
//         Block b = buff.block();
//         if (b != null && b.equals(blk))
//            return buff;
//      }
//      return null;
   }
   
   private Buffer chooseUnpinnedBuffer() {
//      for (Buffer buff : bufferpool)
//         if (!buff.isPinned())
//         return buff;
//      return null;
     Buffer buff = null;
     for (Map.Entry<String, Buffer> entry : bufferPoolMap.entrySet()){
       if(!entry.getValue().isPinned()){
         buff = entry.getValue();
       }
     }
     return buff;
   }
   
   private Buffer chooseMostRecentlyModifiedBuffer(){
    // Logic to get the most recently modified buffer.
    
     int max_log_num = -2;
     Buffer rem_buffer = null;
      
  // Iterate over the buffers present in pool to get the most recently modified buffer.
      for (Map.Entry<String, Buffer> entry : bufferPoolMap.entrySet()){
      Buffer curr_buff = entry.getValue();
      
      // Checking if the buffer present in the buffer pool is unpinned and if the buffer has been modified or not
      if(!curr_buff.isPinned() && curr_buff.getlsn() >= 0){
        // Check if the log sequence number of the current buffer is maximum
        if(max_log_num < curr_buff.getlsn()){
          // Set the buffer index to the current buffer to keep the record of buffer with maximum log sequence number
          rem_buffer = curr_buff;
        }
      }
    }

    return rem_buffer;
   }
   
   /**
   * Determines whether the map has a mapping from
   * the block to some buffer.
   * @paramblk the block to use as a key
   * @return true if there is a mapping; false otherwise
   */
   boolean containsMapping(Block blk) {
   return bufferPoolMap.containsKey(blk.fileName()+"."+blk.number());
   }
   /**
   * Returns the buffer that the map maps the specified block to.
   * @paramblk the block to use as a key
   * @return the buffer mapped to if there is a mapping; null otherwise
   */
   Buffer getMapping(Block blk) {
   return bufferPoolMap.get(blk.fileName()+"."+blk.number());
   }
   

  // Getting Buffer statistics for each buffer.
   public void getStatistics()
  	   {
  		   int buffIndex=0;
  		  
  		   for (Buffer buff : bufferPoolMap.values())
  		   {
  			   ++buffIndex;
  			   System.out.println("Buffer: "+ buff +"  Index: "+ buffIndex + "  Read: "+buff.getBufferReadCount()+"  Write: "+buff.getBufferWriteCount());
  	   }
  	}
}