module Database.Taxi.Segment.Query ( getPostings
                                   ) where

import           Prelude                         hiding (lookup)
import           Control.Applicative             ((<$>))
import           Data.Binary                     (Binary, get)
import           Data.Binary.Get                 (getWord64le, runGet)
import qualified Data.ByteString.Lazy            as LB
import           Data.Maybe                      (fromJust)
import           Data.Set                        (Set)
import qualified Data.Set                        as S
import           System.IO

import           Database.Taxi.Segment.Types

getPostings :: (Binary p, Ord p) => FilePath -> Offset -> IO (Set p)
getPostings segmentPath offset = withFile segmentPath ReadMode getPostings'
  where
    getPostings' :: (Binary p, Ord p) => Handle -> IO (Set p)
    getPostings' handle = do
      hSeek handle AbsoluteSeek (fromIntegral offset)
      term <- readIIEntry handle
      return . S.fromList $ iiPostingsList term

readIIEntry :: Binary p => Handle -> IO (IIEntry p)
readIIEntry handle = fromJust <$> readIIEntryMaybe handle

readIIEntryMaybe :: Binary p => Handle -> IO (Maybe (IIEntry p))
readIIEntryMaybe handle = do
  lenBS <- LB.hGet handle word64Len
  if LB.null lenBS then
    return Nothing
    else let len = runGet getWord64le lenBS
         in do
           termsBS <- LB.hGet handle (fromIntegral len)
           return $ Just $ runGet get termsBS
  where
    word64Len = 8
