import Test.Hspec
import qualified Data.PowerQueueSpec

main :: IO ()
main =
    hspec $
    Data.PowerQueueSpec.spec
