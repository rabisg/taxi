{-|
  Module      : Database.Taxi.Segment.ExternalSegment
  Description : Operations on persistent segment
  Stability   : Experimental
  Maintainer  : guha.rabishankar@gmail.com
-}

module Database.Taxi.Segment.ExternalSegment ( lookup
                                             , delete
                                             , readSegment
                                             , finalize
                                             , merge
                                             ) where

import           Prelude                         hiding (lookup)
import           Control.Applicative             ((<$>))
import           Data.Binary                     (Binary, get)
import           Data.Binary.Get                 (runGet)
import qualified Data.ByteString.Lazy            as LB
import           Data.Maybe                      (fromJust)
import           Data.Set                        (Set)
import qualified Data.Set                        as S
import           Data.Text                       (Text)
import qualified Data.Trie                       as T
import           Data.UUID                       (toString, fromString)
import           System.Directory                (removeFile, renameFile)
import           System.FilePath                 ((</>), (<.>))


import           Database.Taxi.Prelude
import           Database.Taxi.Segment.Constants
import qualified Database.Taxi.Segment.Merge     as M
import           Database.Taxi.Segment.Query
import           Database.Taxi.Segment.Types

readSegment :: FilePath -> String -> IO (ExternalSegment p)
readSegment baseDir segId = do
  dict <- runGet get <$> LB.readFile segFile
  return ExternalSegment { segmentId = fromJust . fromString $ segId
                         , dictionary = dict
                         }
  where
    segFile = baseDir </> segId <.> dataExtension

lookup :: (Ord p, Binary p)
          => ExternalSegment p -> FilePath ->  Text -> IO (Set p)
lookup segment baseDir term = case maybeOffset of
  Nothing -> return S.empty
  Just offset -> getPostings segmentFile offset
  where
    maybeOffset = T.lookup termBS dict
    termBS = toByteString term
    dict = unDict . dictionary $ segment
    segmentFile = baseDir
                  </> toString (segmentId segment)
                  <.> dataExtension


merge :: ExternalSegment p -> ExternalSegment p
         -> Set doc
         -> (Set p -> Set doc -> (Set p, Set doc))
         -> IO (ExternalSegment p, Set doc)
merge = M.merge


delete :: FilePath -> ExternalSegment p -> IO ()
delete baseDir seg = removeFile $ baseDir </> fileName seg
  where fileName = toString . segmentId


finalize :: FilePath -> ExternalSegment p -> IO ()
finalize baseDir seg = mapM_ moveFile [ fileName <.> dataExtension
                                      , fileName <.> indexExtension
                                      ]

  where fileName = toString . segmentId $ seg
        moveFile file = renameFile
                        (baseDir </> tempDir </> file)
                        (baseDir </> file)
