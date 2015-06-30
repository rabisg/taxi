{-|
  Module      : Database.Taxi.Operations
  Description : Type definitions
  Stability   : Experimental
  Maintainer  : guha.rabishankar@gmail.com
-}

module Database.Taxi.Operations ( addDocument
                                , checkpoint
                                , deleteDocument
                                , getSegmentIds
                                , initIndex
                                , mergeSegments
                                , query
                                ) where

import           Control.Concurrent.MVar
import           Control.Monad                         (void)
import qualified Data.Map.Strict                       as Map
import           Data.Set                              (Set)
import qualified Data.Set                              as Set
import           Data.Maybe                            (fromJust)

import           Database.Taxi.Prelude
import           Database.Taxi.Types
import           Database.Taxi.Segment.ExternalSegment (merge, delete, finalize)
import qualified Database.Taxi.Segment.InMemorySegment as MemSeg
import           Database.Taxi.Segment.Types           (SegmentId, segmentId)
import           Database.Taxi.SegmentSet.Types
import qualified Database.Taxi.SegmentSet.Operations   as SegSet

initIndex :: Document doc p => IndexConfig -> IO (Index doc p)
initIndex conf = do
  segSet <- SegSet.init (baseDirectory conf) >>= newMVar
  flushSeg <- newEmptyMVar
  return Index { config = conf
               , segments = segSet
               , flushSegment = flushSeg
               }

-- TODO Add log entry
addDocument :: Document doc p => Index doc p -> doc -> IO ()
addDocument index doc = modifyMVarPure (segments index)
                        $ \segSet -> foldr f segSet terms
  where
    f (t, p) ss = SegSet.insert ss (unKey t) (Posting doc p)
    terms = getTerms doc

-- TODO Add log entry
deleteDocument :: Document doc p => Index doc p -> doc -> IO ()
deleteDocument index doc = modifyMVarPure (segments index) (`SegSet.delete` doc)

query :: Document doc p => Index doc p -> Term doc -> IO [Posting doc p]
query index (Term t) = do
  segSet <- readMVar (segments index)
  maybeFlushSeg <- tryReadMVar (flushSegment index)
  let ps' = case maybeFlushSeg of
        Nothing -> Set.empty
        Just flushSeg -> MemSeg.lookup flushSeg t
  ps <- SegSet.lookup segSet t prune
  return $ Set.elems (ps `Set.union` ps')
  where
    prune delDocs ps = ps `Set.difference`
                       Set.map (`Posting` undefined) delDocs

checkpoint ::Document doc p => Index doc p -> IO ()
checkpoint index = do
  ss <- takeMVar (segments index)
  flushBegin <- tryPutMVar (flushSegment index) (inMemorySegment ss)
  if flushBegin then
    do
      putMVar (segments index) $ ss { inMemorySegment = MemSeg.empty }

      let memSeg = inMemorySegment ss
          delDocs = deletedDocs ss
          baseDir = segSetBaseDir ss
      (newSeg, gcDocs) <- MemSeg.flush memSeg baseDir delDocs gcFunc

      -- Critical Block
      segSet <- takeMVar (segments index)
      -- Write the deletedDocs to disk
      let newDelDocs =  deletedDocs segSet `Set.difference` gcDocs
      SegSet.writeDeletedDocs (segSetBaseDir segSet) newDelDocs
      -- Move new segment
      finalize (segSetBaseDir segSet) newSeg
      -- Modify Index Handle
      putMVar (segments index) $
        segSet { deletedDocs = newDelDocs
               , externalSegments = Map.insert (segmentId newSeg) newSeg $
                                    externalSegments segSet
               }
      -- End critical block

      -- Empty the Flush MVar for the next operation
      void . takeMVar $ flushSegment index
    else putMVar (segments index) ss


getSegmentIds :: Index doc p -> IO [SegmentId]
getSegmentIds index = do
  segSet <- readMVar (segments index)
  return $ SegSet.getSegmentIds segSet


mergeSegments :: Document doc p => Index doc p -> SegmentId -> SegmentId -> IO ()
mergeSegments index sid sid' = do
  ss <- readMVar (segments index)
  let seg  = fromJust $ Map.lookup sid (externalSegments ss)
      seg' = fromJust $ Map.lookup sid' (externalSegments ss)
      delDocs = deletedDocs ss
  (newSeg, gcDocs) <- merge seg seg' delDocs gcFunc

  -- TODO Add log entry for merge operations
  -- Along with new and old segments, write the garbage collected docs

  -- Critical Block
  segSet <- takeMVar (segments index)
  -- Delete old segments
  mapM_ (delete $ segSetBaseDir segSet) [seg, seg']
  -- Move new segment
  finalize (segSetBaseDir segSet) newSeg
  -- Modify Index Handle
  putMVar (segments index) $
    segSet { deletedDocs = deletedDocs segSet `Set.difference` gcDocs
           , externalSegments = Map.delete sid .
                                Map.delete sid' .
                                Map.insert (segmentId newSeg) newSeg $
                                externalSegments segSet
           }

-- FIXME Is there a better way to do this
gcFunc :: Ord doc => Set (Posting doc p) -> Set doc ->
          (Set (Posting doc p), Set doc)
gcFunc ps delDocs =
  (ps `Set.difference` delDocsPosting, delDocs `Set.difference` psDocs)
  where
    delDocsPosting = Set.map (`Posting` undefined) delDocs
    psDocs = Set.map postingDocId ps
