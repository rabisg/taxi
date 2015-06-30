module Database.Taxi ( IndexConfig(..)
                     , Index
                     , module Database.Taxi.Operations
                     , getConfig
                     ) where

import Database.Taxi.Types
import Database.Taxi.Operations

getConfig :: Index doc p -> IndexConfig
getConfig = config
