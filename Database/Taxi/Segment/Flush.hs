{-|
  Module      : Database.Taxi.Segment.Types
  Description : Type definitions
  Stability   : Experimental
  Maintainer  : guha.rabishankar@gmail.com
-}

module Database.Taxi.Segment.Flush ( flush
                                   ) where

import           Data.Binary          (Binary, put)
import           Data.Binary.Put      (runPut, putWord64le)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as LB
import           Data.Set (Set)
import qualified Data.Set as S
import qualified Data.Trie as T
import           Data.UUID            (toString)
import           Data.UUID.V4
import           System.IO            (Handle, withFile, IOMode(..))
import           System.FilePath      ((</>), (<.>))

import Database.Taxi.Segment.Constants
import Database.Taxi.Segment.Types

buildII :: Ord doc
           =>[(B.ByteString, Set p)]
           -> (Set p -> (Set p, Set doc))
           -> ([IIEntry p], Set doc)
buildII ts prune =  (iiEntrys, S.unions gcDocs)
  where
    (iiEntrys, gcDocs) = unzip $ map build ts
    build (a, p) =
      let gcPostings = prune p
      in (IIEntry a (S.elems . fst $ gcPostings), snd gcPostings)


buildSegment :: [IIEntry p] -> [Offset]
             -> IO (ExternalSegment p)
buildSegment ii offs = do
  sid <- nextRandom
  return $ ExternalSegment sid dict
  where
    dict = Dictionary . T.fromList $ zip ts offs
    ts = map iiTerm ii

flush :: (Binary p, Binary doc, Ord doc)
         => InMemorySegment p
         -> FilePath
         -> Set doc
         -> (Set p -> Set doc -> (Set p, Set doc))
         -> IO (ExternalSegment p, Set doc)
flush (InMemorySegment trie) baseDir delDocs prune = do
  seg <- buildSegment ii offsets
  writeSegment baseDir seg iiBS
  return (seg, gcDocs)
  where
    (ii, gcDocs) = buildII (T.toList trie) (`prune` delDocs)
    iiBS = map (runPut . put) ii
    offsets = scanl accum headerLength iiBS
    accum len term = len + word64Len + LB.length term
    word64Len = 8
    headerLength = 0


writeSegment :: FilePath -> ExternalSegment p -> [LB.ByteString] -> IO ()
writeSegment baseDir seg iiBS = do
  withFile dataFile WriteMode (writeData iiBS)
  writeDict (dictionary seg) dictFile
  where
    dataFile = baseDir </> sid <.> dataExtension
    dictFile = baseDir </> sid <.> indexExtension
    sid = toString . segmentId $ seg

writeDict :: Dictionary p -> FilePath -> IO ()
writeDict dict fp = LB.writeFile fp (runPut . put $ dict)

writeData :: [LB.ByteString] -> Handle -> IO ()
writeData bs handle = mapM_ (LB.hPut handle) bs'
  where
    bs' = zipWith LB.append len bs
    len = map (runPut . putWord64le . fromIntegral . LB.length) bs
