import Test.Hspec
import qualified Data.PowerQueue.Backend.LevelMemSpec

main :: IO ()
main =
    hspec $
    Data.PowerQueue.Backend.LevelMemSpec.spec
