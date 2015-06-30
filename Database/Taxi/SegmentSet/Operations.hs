module Database.Taxi.SegmentSet.Operations where

import           Control.Applicative                   ((<$>))
import           Control.Concurrent.Async              (mapConcurrently)
import qualified Data.ByteString.Lazy                  as LB
import           Data.Binary                           (Binary, put, get)
import           Data.Binary.Get                       (runGet)
import           Data.Binary.Put                       (runPut)
import qualified Data.Map.Strict                       as Map
import           Data.Maybe                            (fromJust)
import           Data.Text                             (Text)
import           Data.Set                              (Set)
import qualified Data.Set                              as Set
import           Data.UUID                             (fromString)
import           System.FilePath                       ((</>))

import           Database.Taxi.Segment.Constants
import           Database.Taxi.Segment.Types           (SegmentId)
import qualified Database.Taxi.Segment.InMemorySegment as MemSeg
import qualified Database.Taxi.Segment.ExternalSegment as ExtSeg
import           Database.Taxi.SegmentSet.Types

init :: Binary doc => FilePath -> IO (SegmentSet doc p)
init baseDir = do
  delDocs <- readDeletedDocs baseDir
  segIds <- return [] -- undefined
  externalSegs <- mapM (ExtSeg.readSegment baseDir) segIds
  let uuids = map (fromJust . fromString) segIds
      extSegs = Map.fromList $ zip uuids externalSegs
  return SegmentSet { segSetBaseDir = baseDir
                    , inMemorySegment = MemSeg.empty
                    , externalSegments = extSegs
                    , deletedDocs = delDocs
                    }

insert :: Ord p => SegmentSet doc p -> Text -> p -> SegmentSet doc p
insert ss term p = ss {
  inMemorySegment = MemSeg.insert (inMemorySegment ss) term p
  }

lookup :: (Binary p, Ord p)
          => SegmentSet doc p -> Text
          -> (Set doc -> Set p -> Set p)
          -> IO (Set p)
lookup ss term f = do
  let memVals = MemSeg.lookup (inMemorySegment ss) term
      extSegs = Map.elems $ externalSegments ss
  extVals <- mapConcurrently (\seg -> ExtSeg.lookup seg baseDir term) extSegs
  return . removeDeleted $ foldr Set.union memVals extVals
  where
    baseDir = segSetBaseDir ss
    removeDeleted = f $ deletedDocs ss

delete :: Ord doc => SegmentSet doc p -> doc -> SegmentSet doc p
delete ss doc =  ss {
  deletedDocs = Set.insert doc (deletedDocs ss)
  }

readDeletedDocs :: Binary doc => FilePath -> IO (Set doc)
readDeletedDocs baseDir =
  runGet get <$> LB.readFile delFilePath
  where
    delFilePath = baseDir </> deletedDocsFile

writeDeletedDocs :: Binary doc => FilePath -> Set doc -> IO ()
writeDeletedDocs baseDir delDocs =
  LB.writeFile delFilePath (runPut . put $ delDocs)
  where
    delFilePath = baseDir </> deletedDocsFile

getSegmentIds :: SegmentSet doc p -> [SegmentId]
getSegmentIds = Map.keys . externalSegments
